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
package org.apache.james.mailbox.jpa;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.apache.james.mailbox.MailboxConstants;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.jpa.mail.JPAMailboxMapper;
import org.apache.james.mailbox.jpa.mail.JPAMessageMapper;
import org.apache.james.mailbox.jpa.user.JPASubscriptionMapper;
import org.apache.james.mailbox.store.MailboxSessionMapperFactory;
import org.apache.james.mailbox.store.UidProvider;
import org.apache.james.mailbox.store.mail.MailboxMapper;
import org.apache.james.mailbox.store.mail.MessageMapper;
import org.apache.james.mailbox.store.user.SubscriptionMapper;

/**
 * JPA implementation of {@link MailboxSessionMapperFactory}
 *
 */
public class JPAMailboxSessionMapperFactory extends MailboxSessionMapperFactory<Long> {

    private final EntityManagerFactory entityManagerFactory;
    private final char delimiter;
    private UidProvider<Long> uidGenerator;

    public JPAMailboxSessionMapperFactory(EntityManagerFactory entityManagerFactory, UidProvider<Long> uidProvider) {
        this(entityManagerFactory, uidProvider, MailboxConstants.DEFAULT_DELIMITER);
    }

    public JPAMailboxSessionMapperFactory(EntityManagerFactory entityManagerFactory, UidProvider<Long> uidGenerator, char delimiter) {
        this.entityManagerFactory = entityManagerFactory;
        this.delimiter = delimiter;
        this.uidGenerator = uidGenerator;
    }
    
    @Override
    public MailboxMapper<Long> createMailboxMapper(MailboxSession session) {
        return new JPAMailboxMapper(entityManagerFactory, delimiter);
    }

    @Override
    public MessageMapper<Long> createMessageMapper(MailboxSession session) {
        return new JPAMessageMapper(session, entityManagerFactory, uidGenerator);
    }

    @Override
    public SubscriptionMapper createSubscriptionMapper(MailboxSession session) {
        return new JPASubscriptionMapper(entityManagerFactory);
    }

    /**
     * Return a new {@link EntityManager} instance
     * 
     * @return manager
     */
    public EntityManager createEntityManager() {
        return entityManagerFactory.createEntityManager();
    }

}