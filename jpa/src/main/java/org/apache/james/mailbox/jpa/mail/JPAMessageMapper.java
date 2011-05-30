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
package org.apache.james.mailbox.jpa.mail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceException;
import javax.persistence.Query;

import org.apache.james.mailbox.MailboxException;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.MessageMetaData;
import org.apache.james.mailbox.MessageRange;
import org.apache.james.mailbox.MessageRange.Type;
import org.apache.james.mailbox.jpa.mail.model.JPAMailbox;
import org.apache.james.mailbox.jpa.mail.model.openjpa.AbstractJPAMessage;
import org.apache.james.mailbox.jpa.mail.model.openjpa.JPAMessage;
import org.apache.james.mailbox.jpa.mail.model.openjpa.JPAStreamingMessage;
import org.apache.james.mailbox.store.MessageSearchIndex;
import org.apache.james.mailbox.store.mail.AbstractMessageMapper;
import org.apache.james.mailbox.store.mail.MessageMapper;
import org.apache.james.mailbox.store.mail.SimpleMessageMetaData;
import org.apache.james.mailbox.store.mail.model.Mailbox;
import org.apache.james.mailbox.store.mail.model.Message;
import org.apache.openjpa.persistence.ArgumentException;

/**
 * JPA implementation of a {@link MessageMapper}. This class is not thread-safe!
 */
public class JPAMessageMapper extends AbstractMessageMapper<Long> implements MessageMapper<Long> {
    protected EntityManagerFactory entityManagerFactory;
    protected EntityManager entityManager;
    
    public JPAMessageMapper(final MailboxSession session, MessageSearchIndex<Long> index, final EntityManagerFactory entityManagerFactory) {
        super(session, index);
        this.entityManagerFactory = entityManagerFactory;
    }


    /**
     * Return the currently used {@link EntityManager} or a new one if none exists.
     * 
     * @return entitymanger
     */
    public EntityManager getEntityManager() {
        if (entityManager != null)
            return entityManager;
        entityManager = entityManagerFactory.createEntityManager();
        return entityManager;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.transaction.AbstractTransactionalMapper#begin()
     */
    protected void begin() throws MailboxException {
        try {
            getEntityManager().getTransaction().begin();
        } catch (PersistenceException e) {
            throw new MailboxException("Begin of transaction failed", e);
        }
    }

    /**
     * Commit the Transaction and close the EntityManager
     */
    protected void commit() throws MailboxException {
        try {
            getEntityManager().getTransaction().commit();
        } catch (PersistenceException e) {
            throw new MailboxException("Commit of transaction failed",e);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.transaction.AbstractTransactionalMapper#rollback()
     */
    protected void rollback() throws MailboxException {
        EntityTransaction transaction = entityManager.getTransaction();
        // check if we have a transaction to rollback
        if (transaction.isActive()) {
            getEntityManager().getTransaction().rollback();
        }
    }

    /**
     * Close open {@link EntityManager}
     */
    public void endRequest() {
        if (entityManager != null) {
            if (entityManager.isOpen())
                entityManager.close();
            entityManager = null;
        }
    }
    /**
     * @see org.apache.james.mailbox.store.mail.MessageMapper#findInMailbox(org.apache.james.mailbox.MessageRange)
     */
    public void findInMailbox(Mailbox<Long> mailbox, MessageRange set, MailboxMembershipCallback<Long> callback) throws MailboxException {
        try {
            List<Message<Long>> results;
            long from = set.getUidFrom();
            final long to = set.getUidTo();
            final int batchSize = set.getBatchSize();
            final Type type = set.getType();

            // when batch is specified fetch data in chunks and send back in
            // batches
            do {
                switch (type) {
                default:
                case ALL:
                    results = findMessagesInMailbox(mailbox, batchSize);
                    break;
                case FROM:
                    results = findMessagesInMailboxAfterUID(mailbox, from, batchSize);
                    break;
                case ONE:
                    results = findMessagesInMailboxWithUID(mailbox, from);
                    break;
                case RANGE:
                    results = findMessagesInMailboxBetweenUIDs(mailbox, from, to, batchSize);
                    break;
                }

                if (results.size() > 0) {
                    callback.onMailboxMembers(results);

                    // move the start UID behind the last fetched message UID
                    from = results.get(results.size() - 1).getUid() + 1;
                }

            } while (results.size() > 0 && batchSize > 0);

        } catch (PersistenceException e) {
            throw new MailboxException("Search of MessageRange " + set + " failed in mailbox " + mailbox, e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Message<Long>> findMessagesInMailboxAfterUID(Mailbox<Long> mailbox, long uid, int batchSize) {
        Query query = getEntityManager().createNamedQuery("findMessagesInMailboxAfterUID")
        .setParameter("idParam", mailbox.getMailboxId())
        .setParameter("uidParam", uid);
        
        if(batchSize > 0)
        	query.setMaxResults(batchSize);
        
        return query.getResultList();
    }

    @SuppressWarnings("unchecked")
    private List<Message<Long>> findMessagesInMailboxWithUID(Mailbox<Long> mailbox, long uid) {
        return getEntityManager().createNamedQuery("findMessagesInMailboxWithUID")
        .setParameter("idParam", mailbox.getMailboxId())
        .setParameter("uidParam", uid).setMaxResults(1).getResultList();
    }

    @SuppressWarnings("unchecked")
    private List<Message<Long>> findMessagesInMailboxBetweenUIDs(Mailbox<Long> mailbox, long from, long to, int batchSize) {
        Query query = getEntityManager().createNamedQuery("findMessagesInMailboxBetweenUIDs").setParameter("idParam", mailbox.getMailboxId()).setParameter("fromParam", from).setParameter("toParam", to);

        if (batchSize > 0)
            query.setMaxResults(batchSize);

        return query.getResultList();
    }

    @SuppressWarnings("unchecked")
    private List<Message<Long>> findMessagesInMailbox(Mailbox<Long> mailbox, int batchSize) {
         Query query = getEntityManager().createNamedQuery("findMessagesInMailbox").setParameter("idParam", mailbox.getMailboxId());
         if(batchSize > 0)
        	 query.setMaxResults(batchSize);
         return query.getResultList();
    }


    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.AbstractMessageMapper#expungeMarkedForDeletion(org.apache.james.mailbox.store.mail.model.Mailbox, org.apache.james.mailbox.MessageRange)
     */
    public Map<Long, MessageMetaData> expungeMarkedForDeletion(Mailbox<Long> mailbox, final MessageRange set) throws MailboxException {
        try {
            final Map<Long, MessageMetaData> data;
            final List<Message<Long>> results;
            final long from = set.getUidFrom();
            final long to = set.getUidTo();
            
            switch (set.getType()) {
                case ONE:
                    results = findDeletedMessagesInMailboxWithUID(mailbox, from);
                    data = createMetaData(results);
                    deleteDeletedMessagesInMailboxWithUID(mailbox, from);
                    break;
                case RANGE:
                    results = findDeletedMessagesInMailboxBetweenUIDs(mailbox, from, to);
                    data = createMetaData(results);
                    deleteDeletedMessagesInMailboxBetweenUIDs(mailbox, from, to);
                    break;
                case FROM:
                    results = findDeletedMessagesInMailboxAfterUID(mailbox, from);
                    data = createMetaData(results);
                    deleteDeletedMessagesInMailboxAfterUID(mailbox, from);
                    break;
                default:
                case ALL:
                    results = findDeletedMessagesInMailbox(mailbox);
                    data = createMetaData(results);
                    deleteDeletedMessagesInMailbox(mailbox);
                    break;
            }
            
            return data;
        } catch (PersistenceException e) {
            throw new MailboxException("Search of MessageRange " + set + " failed in mailbox " + mailbox, e);
        }
    }
    
    private Map<Long, MessageMetaData> createMetaData(List<Message<Long>> uids) {
        final Map<Long, MessageMetaData> data = new HashMap<Long, MessageMetaData>();
        for (int i = 0; i < uids.size(); i++) {
            Message<Long> m = uids.get(i);
            data.put(m.getUid(),  new SimpleMessageMetaData(m));
        }
        return data;
    }


    private int deleteDeletedMessagesInMailbox(Mailbox<Long> mailbox) {
        return getEntityManager().createNamedQuery("deleteDeletedMessagesInMailbox").setParameter("idParam", mailbox.getMailboxId()).executeUpdate();
    }

    private int deleteDeletedMessagesInMailboxAfterUID(Mailbox<Long> mailbox, long uid) {
        return getEntityManager().createNamedQuery("deleteDeletedMessagesInMailboxAfterUID")
        .setParameter("idParam", mailbox.getMailboxId())
        .setParameter("uidParam", uid).executeUpdate();
    }

    private int deleteDeletedMessagesInMailboxWithUID(Mailbox<Long> mailbox, long uid) {
        return getEntityManager().createNamedQuery("deleteDeletedMessagesInMailboxWithUID")
        .setParameter("idParam", mailbox.getMailboxId())
        .setParameter("uidParam", uid).executeUpdate();
    }

    private int deleteDeletedMessagesInMailboxBetweenUIDs(Mailbox<Long> mailbox, long from, long to) {
        return getEntityManager().createNamedQuery("deleteDeletedMessagesInMailboxBetweenUIDs")
        .setParameter("idParam", mailbox.getMailboxId())
        .setParameter("fromParam", from)
        .setParameter("toParam", to).executeUpdate();
    }

    
    @SuppressWarnings("unchecked")
    private List<Message<Long>> findDeletedMessagesInMailbox(Mailbox<Long> mailbox) {
        return getEntityManager().createNamedQuery("findDeletedMessagesInMailbox").setParameter("idParam", mailbox.getMailboxId()).getResultList();
    }

    @SuppressWarnings("unchecked")
    private List<Message<Long>> findDeletedMessagesInMailboxAfterUID(Mailbox<Long> mailbox, long uid) {
        return getEntityManager().createNamedQuery("findDeletedMessagesInMailboxAfterUID")
        .setParameter("idParam", mailbox.getMailboxId())
        .setParameter("uidParam", uid).getResultList();
    }

    @SuppressWarnings("unchecked")
    private List<Message<Long>> findDeletedMessagesInMailboxWithUID(Mailbox<Long> mailbox, long uid) {
        return getEntityManager().createNamedQuery("findDeletedMessagesInMailboxWithUID")
        .setParameter("idParam", mailbox.getMailboxId())
        .setParameter("uidParam", uid).setMaxResults(1).getResultList();
    }

    @SuppressWarnings("unchecked")
    private List<Message<Long>> findDeletedMessagesInMailboxBetweenUIDs(Mailbox<Long> mailbox, long from, long to) {
        return getEntityManager().createNamedQuery("findDeletedMessagesInMailboxBetweenUIDs")
        .setParameter("idParam", mailbox.getMailboxId())
        .setParameter("fromParam", from)
        .setParameter("toParam", to).getResultList();
    }

    /**
     * @see org.apache.james.mailbox.store.mail.MessageMapper#countMessagesInMailbox()
     */
    public long countMessagesInMailbox(Mailbox<Long> mailbox) throws MailboxException {
        try {
            return (Long) getEntityManager().createNamedQuery("countMessagesInMailbox").setParameter("idParam", mailbox.getMailboxId()).getSingleResult();
        } catch (PersistenceException e) {
            throw new MailboxException("Count of messages failed in mailbox " + mailbox, e);
        }
    }

    /**
     * @see org.apache.james.mailbox.store.mail.MessageMapper#countUnseenMessagesInMailbox()
     */
    public long countUnseenMessagesInMailbox(Mailbox<Long> mailbox) throws MailboxException {
        try {
            return (Long) getEntityManager().createNamedQuery("countUnseenMessagesInMailbox").setParameter("idParam", mailbox.getMailboxId()).getSingleResult();
        } catch (PersistenceException e) {
            throw new MailboxException("Count of useen messages failed in mailbox " + mailbox, e);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.MessageMapper#delete(java.lang.Object, org.apache.james.mailbox.store.mail.model.MailboxMembership)
     */
    public void delete(Mailbox<Long> mailbox, Message<Long> message) throws MailboxException {
        try {
            getEntityManager().remove(message);
        } catch (PersistenceException e) {
            throw new MailboxException("Delete of message " + message + " failed in mailbox " + mailbox, e);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.MessageMapper#findFirstUnseenMessageUid(org.apache.james.mailbox.store.mail.model.Mailbox)
     */
    @SuppressWarnings("unchecked")
    public Long findFirstUnseenMessageUid(Mailbox<Long> mailbox)  throws MailboxException {
        try {
            Query query = getEntityManager().createNamedQuery("findUnseenMessagesInMailboxOrderByUid").setParameter("idParam", mailbox.getMailboxId());
            query.setMaxResults(1);
            List<Message<Long>> result = query.getResultList();
            if (result.isEmpty()) {
                return null;
            } else {
                return result.get(0).getUid();
            }
        } catch (PersistenceException e) {
            throw new MailboxException("Search of first unseen message failed in mailbox " + mailbox, e);
        }
    }

    /**
     * @see org.apache.james.mailbox.store.mail.MessageMapper#findRecentMessagesInMailbox()
     */
    @SuppressWarnings("unchecked")
    public List<Message<Long>> findRecentMessagesInMailbox(Mailbox<Long> mailbox) throws MailboxException {
        try {
            Query query = getEntityManager().createNamedQuery("findRecentMessagesInMailbox").setParameter("idParam", mailbox.getMailboxId());
            return query.getResultList();
        } catch (PersistenceException e) {
            throw new MailboxException("Search of recent messages failed in mailbox " + mailbox, e);
        }
    }



    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.AbstractMessageMapper#calculateHigestModSeq(org.apache.james.mailbox.store.mail.model.Mailbox)
     */
    protected long calculateHigestModSeq(Mailbox<Long> mailbox) throws MailboxException {
        try {
            final long modSeq = (Long) entityManager.createNamedQuery("findHighestModSeqInMailbox").setParameter("idParam", mailbox.getMailboxId()).setMaxResults(1).getSingleResult();
            return modSeq;
        } catch (NoResultException e) {
            return 0;
        } catch (PersistenceException e) {
            throw new MailboxException("Unable to retrieve higehst mod-sequence for mailbox " + mailbox);
        }
    }


    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.AbstractMessageMapper#calculateLastUid(org.apache.james.mailbox.store.mail.model.Mailbox)
     */
    protected long calculateLastUid(Mailbox<Long> mailbox) throws MailboxException {
        try {
            final long uid = (Long) entityManager.createNamedQuery("findLastUidInMailbox").setParameter("idParam", mailbox.getMailboxId()).setMaxResults(1).getSingleResult();
            return uid;
        } catch (NoResultException e) {
            return 0;
        } catch (PersistenceException e) {
            throw new MailboxException("Unable to retrieve last uid for mailbox " + mailbox);
        }
    }


    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.AbstractMessageMapper#copy(org.apache.james.mailbox.store.mail.model.Mailbox, long, long, org.apache.james.mailbox.store.mail.model.Message)
     */
    protected void copy(Mailbox<Long> mailbox, long uid, long modSeq, Message<Long> original) throws MailboxException {
        Message<Long> copy;
        if (original instanceof JPAStreamingMessage) {
            copy = new JPAStreamingMessage((JPAMailbox) mailbox, uid, modSeq, original);
        } else {
            copy = new JPAMessage((JPAMailbox) mailbox, uid, modSeq, original);
        }
        save(mailbox, copy);        
    }


    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.AbstractMessageMapper#save(org.apache.james.mailbox.store.mail.model.Mailbox, org.apache.james.mailbox.store.mail.model.Message)
     */
    protected void save(Mailbox<Long> mailbox, Message<Long> message) throws MailboxException {

        try {
            
            // We need to reload a "JPA attached" mailbox, because the provide mailbox is already "JPA detached"
            // If we don't this, we will get an org.apache.openjpa.persistence.ArgumentException.
            ((AbstractJPAMessage) message).setMailbox(getEntityManager().find(JPAMailbox.class, mailbox.getMailboxId()));
            
            getEntityManager().persist(message);
                    
        } catch (PersistenceException e) {
            throw new MailboxException("Save of message " + message + " failed in mailbox " + mailbox, e);
        } catch (ArgumentException e) {
            throw new MailboxException("Save of message " + message + " failed in mailbox " + mailbox, e);
        }        
    }


    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.AbstractMessageMapper#saveSequences(org.apache.james.mailbox.store.mail.model.Mailbox, long, long)
     */
    protected void saveSequences(Mailbox<Long> mailbox, long lastUid, long highestModSeq) throws MailboxException {
        try {
            getEntityManager().createNamedQuery("updateSequences")
            .setParameter("idParam", mailbox.getMailboxId())
            .setParameter("lastKnownUidParam", lastUid)
            .setParameter("lastKnownHighestModSeqParam", highestModSeq).executeUpdate();      
        } catch (PersistenceException e) {
            throw new MailboxException("Save of sequences for mailbox " + mailbox + " failed", e);
        }
    }
}
