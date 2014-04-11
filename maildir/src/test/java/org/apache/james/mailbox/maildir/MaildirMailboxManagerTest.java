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
package org.apache.james.mailbox.maildir;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.FileUtils;
import org.apache.james.mailbox.AbstractMailboxManagerTest;
import org.apache.james.mailbox.acl.GroupMembershipResolver;
import org.apache.james.mailbox.acl.MailboxACLResolver;
import org.apache.james.mailbox.acl.SimpleGroupMembershipResolver;
import org.apache.james.mailbox.acl.UnionMailboxACLResolver;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.exception.MailboxExistsException;
import org.apache.james.mailbox.store.JVMMailboxPathLocker;
import org.apache.james.mailbox.store.StoreMailboxManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * MaildirMailboxManagerTest that extends the StoreMailboxManagerTest.
 */
public class MaildirMailboxManagerTest extends AbstractMailboxManagerTest {
    
    private static final String MAILDIR_HOME = "target/Maildir";

    /**
     * Setup the mailboxManager.
     * 
     * @throws Exception
     */
    @Before
    public void setup() throws Exception {
        assertFalse("Maildir tests work only on non-windows systems. So skip the test",
                OsDetector.isWindows());
        deleteMaildirTestDirectory();
        createMailboxManager();
    }
    
    /**
     * Delete Maildir directory after test.
     * 
     * @throws IOException 
     */
    @After
    public void tearDown() throws IOException {
        if (OsDetector.isWindows()) {
            return;
        }
        deleteMaildirTestDirectory();
    }

    /**
     * TODO Tests fail with /%user configuration.
     * @throws MailboxException
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    // This is expected as the there are many users which have the same localpart
    @Test(expected = MailboxExistsException.class)
    public void testListSimpleUserMaildirStoreConfig() throws MailboxException, UnsupportedEncodingException, IOException {
        createMailboxManager("/%user");
        super.testList();
    }

    /**
     * TODO Tests fail with /%fulluser configuration.
     * @throws MailboxException
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    @Test
    public void testListFulluserMaildirStoreConfig() throws MailboxException, UnsupportedEncodingException, IOException {
        createMailboxManager("/%fulluser");
        super.testList();
    }

    /**
     * @throws org.apache.james.mailbox.exception.MailboxException
     * @see org.apache.james.mailbox.MailboxManagerTest#createMailboxManager()
     */
    @Override
    protected void createMailboxManager() throws MailboxException {
        createMailboxManager("/%domain/%user");
    }

    private void createMailboxManager(String maildirStoreConfiguration) throws MailboxException {
        try {
            deleteMaildirTestDirectory();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        MaildirStore store = new MaildirStore(MAILDIR_HOME + maildirStoreConfiguration, new JVMMailboxPathLocker());
        MaildirMailboxSessionMapperFactory mf = new MaildirMailboxSessionMapperFactory(store);

        MailboxACLResolver aclResolver = new UnionMailboxACLResolver();
        GroupMembershipResolver groupMembershipResolver = new SimpleGroupMembershipResolver();

        StoreMailboxManager<Integer> manager = new StoreMailboxManager<Integer>(mf, null, new JVMMailboxPathLocker(), aclResolver, groupMembershipResolver);
        manager.init();
        setMailboxManager(manager);
    }
   
    /**
     * Utility method to delete the test Maildir Directory.
     * 
     * @throws IOException
     */
    private void deleteMaildirTestDirectory() throws IOException {
        FileUtils.deleteDirectory(new File(MAILDIR_HOME));
    }
    
}
