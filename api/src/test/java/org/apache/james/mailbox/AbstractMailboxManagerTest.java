/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/
package org.apache.james.mailbox;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;

import javax.mail.Flags;

import static org.junit.Assert.*;

import org.apache.james.mailbox.exception.BadCredentialsException;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.mock.MockMailboxManager;
import org.apache.james.mailbox.model.MailboxConstants;
import org.apache.james.mailbox.model.MailboxPath;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Test the {@link StoreMailboxManager} methods that 
 * are not covered by the protocol-tester suite.
 * 
 * This class needs to be extended by the different mailbox 
 * implementations which are responsible to setup and 
 * implement the test methods.
 * 
 */
public abstract class AbstractMailboxManagerTest {
    
    private final static String USER_1 = "USER_1";
    private final static String USER_2 = "USER_2";

    /**
     * The mailboxManager that needs to get instanciated
     * by the mailbox implementations.
     */
    protected MailboxManager mailboxManager;
    
    @Test
    public void testBasicOperations() throws BadCredentialsException, MailboxException, UnsupportedEncodingException {

        setMailboxManager(new MockMailboxManager(getMailboxManager()).getMockMailboxManager());
        
        MailboxSession session = getMailboxManager().createSystemSession(USER_1, LoggerFactory.getLogger("Mock"));
        assertEquals(USER_1, session.getUser().getUserName());
        
        getMailboxManager().startProcessingRequest(session);
        
        MailboxPath inbox = MailboxPath.inbox(session);
        assertFalse(getMailboxManager().mailboxExists(inbox, session));
        
        getMailboxManager().createMailbox(inbox, session);
        assertTrue(getMailboxManager().mailboxExists(inbox, session));
        
        try {
            getMailboxManager().createMailbox(inbox, session);
            fail();
        } catch (MailboxException e) {
            // mailbox already exists!
        }
        
        MailboxPath inboxSubMailbox = new MailboxPath(inbox, "INBOX.Test");
        assertFalse(getMailboxManager().mailboxExists(inboxSubMailbox, session));
        
        getMailboxManager().createMailbox(inboxSubMailbox, session);
        assertTrue(getMailboxManager().mailboxExists(inboxSubMailbox, session));
        
        getMailboxManager().deleteMailbox(inbox, session);
        assertFalse(getMailboxManager().mailboxExists(inbox, session));
        
        assertTrue(getMailboxManager().mailboxExists(inboxSubMailbox, session));
        
        getMailboxManager().deleteMailbox(inboxSubMailbox, session);
        assertFalse(getMailboxManager().mailboxExists(inboxSubMailbox, session));

        getMailboxManager().logout(session, false);
        getMailboxManager().endProcessingRequest(session);

        assertFalse(session.isOpen());
    }

    @Test
    public void testConcurrentSessions() throws MailboxException, UnsupportedEncodingException {

        setMailboxManager(new MockMailboxManager(getMailboxManager()).getMockMailboxManager());

        MailboxSession session1 = getMailboxManager().createSystemSession(USER_1, LoggerFactory.getLogger("Mock"));
        assertEquals(USER_1, session1.getUser().getUserName());
        MailboxSession session2 = getMailboxManager().createSystemSession(USER_1, LoggerFactory.getLogger("Mock"));
        assertEquals(USER_1, session2.getUser().getUserName());

        assertNotEquals(session1, session2);

        getMailboxManager().startProcessingRequest(session1);
        getMailboxManager().startProcessingRequest(session2);

        MailboxPath inbox1 = MailboxPath.inbox(session1);
        MailboxPath inbox2 = MailboxPath.inbox(session2);

        getMailboxManager().createMailbox(inbox1, session1);
        
        assertTrue(getMailboxManager().mailboxExists(inbox1, session1));
        assertTrue(getMailboxManager().mailboxExists(inbox2, session2));

        MessageManager mm1 = getMailboxManager().getMailbox(inbox1, session1);
        MessageManager mm2 = getMailboxManager().getMailbox(inbox2, session2);
        Long uid1 = mm1.appendMessage(new ByteArrayInputStream("Subject: test\r\n\r\ntestmail".getBytes()), new Date(), session1, false, new Flags());
        Long uid2 = mm2.appendMessage(new ByteArrayInputStream("Subject: test\r\n\r\ntestmail".getBytes()), new Date(), session2, false, new Flags());

        assertNotEquals(uid1, uid2);

        getMailboxManager().logout(session1, false);
        getMailboxManager().endProcessingRequest(session1);
        getMailboxManager().logout(session2, false);
        getMailboxManager().endProcessingRequest(session2);

        assertFalse(session1.isOpen());
        assertFalse(session2.isOpen());
    }

    @Test
    public void testSubsequentSessions() throws MailboxException, UnsupportedEncodingException {

        setMailboxManager(new MockMailboxManager(getMailboxManager()).getMockMailboxManager());

        MailboxSession session1 = getMailboxManager().createSystemSession(USER_1, LoggerFactory.getLogger("Mock"));
        assertEquals(USER_1, session1.getUser().getUserName());
        getMailboxManager().startProcessingRequest(session1);
        MailboxPath inbox1 = MailboxPath.inbox(session1);
        getMailboxManager().createMailbox(inbox1, session1);
        assertTrue(getMailboxManager().mailboxExists(inbox1, session1));
        MessageManager mm1 = getMailboxManager().getMailbox(inbox1, session1);
        Long uid1 = mm1.appendMessage(new ByteArrayInputStream("Subject: test\r\n\r\ntestmail".getBytes()), new Date(), session1, false, new Flags());

        MailboxSession session2 = getMailboxManager().createSystemSession(USER_1, LoggerFactory.getLogger("Mock"));
        getMailboxManager().startProcessingRequest(session2);
        MailboxPath inbox2 = MailboxPath.inbox(session2);
        assertTrue(getMailboxManager().mailboxExists(inbox2, session2));
        MessageManager mm2 = getMailboxManager().getMailbox(inbox2, session2);
        mm2.getMetaData(true, session2, MessageManager.MetaData.FetchGroup.FIRST_UNSEEN);
        Long uid2 = mm2.appendMessage(new ByteArrayInputStream("Subject: test\r\n\r\ntestmail".getBytes()), new Date(), session2, false, new Flags());
        assertEquals(USER_1, session2.getUser().getUserName());

        assertNotEquals(session1, session2);
        assertNotEquals(uid1, uid2);

        getMailboxManager().logout(session1, false);
        getMailboxManager().endProcessingRequest(session1);
        getMailboxManager().logout(session2, false);
        getMailboxManager().endProcessingRequest(session2);

        assertFalse(session1.isOpen());
        assertFalse(session2.isOpen());
    }

    /**
     * Create some INBOXes and their sub mailboxes and assert list() method.
     * 
     * @throws UnsupportedEncodingException 
     * @throws MailboxException 
     */
    @Test
    public void testList() throws MailboxException, UnsupportedEncodingException {

        setMailboxManager(new MockMailboxManager(getMailboxManager()).getMockMailboxManager());

        MailboxSession mailboxSession = getMailboxManager().createSystemSession("manager", LoggerFactory.getLogger("testList"));
        getMailboxManager().startProcessingRequest(mailboxSession);
        assertEquals(MockMailboxManager.EXPECTED_MAILBOXES_COUNT, getMailboxManager().list(mailboxSession).size());

    }
    
    
    @Test
    public void testCreateSubFolderDirectly() throws BadCredentialsException, MailboxException { 

        MailboxSession session = getMailboxManager().createSystemSession(USER_2, LoggerFactory.getLogger("Test"));
        getMailboxManager().createMailbox(new MailboxPath(MailboxConstants.USER_NAMESPACE, USER_2, "Trash"), session);
        getMailboxManager().createMailbox(new MailboxPath(MailboxConstants.USER_NAMESPACE, USER_2, "INBOX.testfolder"), session);
        
        getMailboxManager().getMailbox(MailboxPath.inbox(session), session).appendMessage(new ByteArrayInputStream("Subject: test\r\n\r\ntestmail".getBytes()), new Date(), session, false, new Flags());

    }

    /**
     * Implement this method to create the mailboxManager.
     * 
     * @return
     * @throws MailboxException 
     */
    protected abstract void createMailboxManager() throws MailboxException;
    
    /**
     * Setter to inject the mailboxManager.
     */
    protected void setMailboxManager(MailboxManager mailboxManager) {
        this.mailboxManager = mailboxManager;
    }

    /**
     * Accessor to the mailboxManager.
     * 
     * @return the mailboxManager instance.
     * @throws IllegalStateException in case of null mailboxManager
     */
    protected MailboxManager getMailboxManager() {
        if (mailboxManager == null) {
            throw new IllegalStateException("Please setMailboxManager with a non null value before requesting getMailboxManager()");
        }
        return mailboxManager;
    }

}
