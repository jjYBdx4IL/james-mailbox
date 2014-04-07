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
package org.apache.james.mailbox.jpa.openjpa;

import javax.persistence.EntityManagerFactory;

import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.jpa.mail.JPAMessageMapper;
import org.apache.james.mailbox.jpa.mail.model.JPAMailbox;
import org.apache.james.mailbox.jpa.mail.model.openjpa.JPAEncryptedMessage;
import org.apache.james.mailbox.jpa.mail.model.JPAMessage;
import org.apache.james.mailbox.jpa.mail.model.openjpa.JPAStreamingMessage;
import org.apache.james.mailbox.model.MessageMetaData;
import org.apache.james.mailbox.store.mail.MessageMapper;
import org.apache.james.mailbox.store.mail.ModSeqProvider;
import org.apache.james.mailbox.store.mail.UidProvider;
import org.apache.james.mailbox.store.mail.model.Mailbox;
import org.apache.james.mailbox.store.mail.model.Message;


public class OpenJPAMessageMapper extends JPAMessageMapper implements MessageMapper<Long> {

    public OpenJPAMessageMapper(MailboxSession session, UidProvider<Long> uidProvider, ModSeqProvider<Long> modSeqProvider, EntityManagerFactory entityManagerFactory) {
        super(session, uidProvider, modSeqProvider, entityManagerFactory);
    }
    /**
     * @return
     * @see org.apache.james.mailbox.store.mail.AbstractMessageMapper#copy(Mailbox,
     *      long, long, Message)
     */
    @Override
    protected MessageMetaData copy(Mailbox<Long> mailbox, long uid, long modSeq, Message<Long> original)
            throws MailboxException {
        Message<Long> copy;
        if (original instanceof JPAStreamingMessage) {
            copy = new JPAStreamingMessage((JPAMailbox) mailbox, uid, modSeq, original);
        } else if (original instanceof JPAEncryptedMessage) {
            copy = new JPAEncryptedMessage((JPAMailbox) mailbox, uid, modSeq, original);
        } else {
            copy = new JPAMessage((JPAMailbox) mailbox, uid, modSeq, original);
        }
        return save(mailbox, copy);
    }

}
