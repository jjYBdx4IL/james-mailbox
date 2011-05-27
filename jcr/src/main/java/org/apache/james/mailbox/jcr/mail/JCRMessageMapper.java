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
package org.apache.james.mailbox.jcr.mail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;

import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.util.ISO9075;
import org.apache.james.mailbox.MailboxException;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.MessageMetaData;
import org.apache.james.mailbox.MessageRange;
import org.apache.james.mailbox.MessageRange.Type;
import org.apache.james.mailbox.jcr.JCRImapConstants;
import org.apache.james.mailbox.jcr.MailboxSessionJCRRepository;
import org.apache.james.mailbox.jcr.mail.model.JCRMailbox;
import org.apache.james.mailbox.jcr.mail.model.JCRMessage;
import org.apache.james.mailbox.store.mail.AbstractMessageMapper;
import org.apache.james.mailbox.store.mail.MessageMapper;
import org.apache.james.mailbox.store.mail.SimpleMessageMetaData;
import org.apache.james.mailbox.store.mail.model.Mailbox;
import org.apache.james.mailbox.store.mail.model.Message;
import org.slf4j.Logger;

/**
 * JCR implementation of a {@link MessageMapper}. The implementation store each message as 
 * a seperate child node under the mailbox
 *
 */
public class JCRMessageMapper extends AbstractMessageMapper<String> implements JCRImapConstants {
    
    /**
     * Store the messages directly in the mailbox: .../mailbox/
     */
    public final static int MESSAGE_SCALE_NONE = 0;

    /**
     * Store the messages under a year directory in the mailbox:
     * .../mailbox/2010/
     */
    public final static int MESSAGE_SCALE_YEAR = 1;

    /**
     * Store the messages under a year/month directory in the mailbox:
     * .../mailbox/2010/05/
     */
    public final static int MESSAGE_SCALE_MONTH = 2;

    /**
     * Store the messages under a year/month/day directory in the mailbox:
     * .../mailbox/2010/05/01/
     */
    public final static int MESSAGE_SCALE_DAY = 3;

    /**
     * Store the messages under a year/month/day/hour directory in the mailbox:
     * .../mailbox/2010/05/02/11
     */
    public final static int MESSAGE_SCALE_HOUR = 4;

    /**
     * Store the messages under a year/month/day/hour/min directory in the
     * mailbox: .../mailbox/2010/05/02/11/59
     */
    public final static int MESSAGE_SCALE_MINUTE = 5;

    private final int scaleType;
    
    private final Logger logger;
    private final MailboxSessionJCRRepository repository;
    protected final MailboxSession mSession;

    
    /**
     * Construct a new {@link JCRMessageMapper} instance
     * 
     * @param repos {@link MailboxSessionJCRRepository} to use
     * @param session {@link MailboxSession} to which the mapper is bound
     * @param logger Log
     */
    public JCRMessageMapper(final MailboxSessionJCRRepository repository, MailboxSession mSession, final Logger logger, int scaleType) {
        this.repository = repository;
        this.mSession = mSession;
        this.logger = logger;
        this.scaleType = scaleType;
    }
    
    public JCRMessageMapper(final MailboxSessionJCRRepository repos, MailboxSession session, final Logger logger) {
        this(repos, session, logger, MESSAGE_SCALE_DAY);
    }

    /**
     * Return the logger
     * 
     * @return logger
     */
    protected Logger getLogger() {
        return logger;
    }
    
    /**
     * Return the JCR Session
     * 
     * @return session
     */
    protected Session getSession() throws RepositoryException{
        return repository.login(mSession);
    }

    /**
     * Begin is not supported by level 1 JCR implementations, however we refresh the session
     */
    protected void begin() throws MailboxException {  
        try {
            getSession().refresh(true);
        } catch (RepositoryException e) {
            // do nothin on refresh
        }
        // Do nothing
    }

    /**
     * Just call save on the underlying JCR Session, because level 1 JCR implementation does not offer Transactions
     */
    protected void commit() throws MailboxException {
        try {
            if (getSession().hasPendingChanges()) {
                getSession().save();
            }
        } catch (RepositoryException e) {
            throw new MailboxException("Unable to commit", e);
        }
    }

    /**
     * Rollback is not supported by level 1 JCR implementations, so just do nothing
     */
    protected void rollback() throws MailboxException {
        try {
            // just refresh session and discard all pending changes
            getSession().refresh(false);
        } catch (RepositoryException e) {
            // just catch on rollback by now
        }
    }

    /**
     * Return the path to the mailbox. This path is escaped to be able to use it in xpath queries
     * 
     * See http://wiki.apache.org/jackrabbit/EncodingAndEscaping
     * 
     * @param mailbox
     * @return
     * @throws ItemNotFoundException
     * @throws RepositoryException
     */
    private String getMailboxPath(Mailbox<String> mailbox) throws ItemNotFoundException, RepositoryException {
        return ISO9075.encodePath(getSession().getNodeByIdentifier(mailbox.getMailboxId()).getPath());
    }
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.james.mailbox.store.mail.MessageMapper#countMessagesInMailbox()
     */
    public long countMessagesInMailbox(Mailbox<String> mailbox) throws MailboxException {
        try {
            // we use order by because without it count will always be 0 in jackrabbit
            String queryString = "/jcr:root" + getMailboxPath(mailbox) + "//element(*,jamesMailbox:message) order by @" + JCRMessage.UID_PROPERTY;
            QueryManager manager = getSession().getWorkspace().getQueryManager();
            QueryResult result = manager.createQuery(queryString, Query.XPATH).execute();
            NodeIterator nodes = result.getNodes();
            long count = nodes.getSize();
            if (count == -1) {
                count = 0;
                while(nodes.hasNext()) {
                    nodes.nextNode();
                    count++;
                }
            } 
            return count;
        } catch (RepositoryException e) {
            throw new MailboxException("Unable to count messages in mailbox " + mailbox, e);
        }
       
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.james.mailbox.store.mail.MessageMapper#countUnseenMessagesInMailbox
     * ()
     */
    public long countUnseenMessagesInMailbox(Mailbox<String> mailbox) throws MailboxException {
        
        try {
            // we use order by because without it count will always be 0 in jackrabbit
            String queryString = "/jcr:root" + getMailboxPath(mailbox) + "//element(*,jamesMailbox:message)[@" + JCRMessage.SEEN_PROPERTY +"='false'] order by @" + JCRMessage.UID_PROPERTY;
            QueryManager manager = getSession().getWorkspace().getQueryManager();
            QueryResult result = manager.createQuery(queryString, Query.XPATH).execute();
            NodeIterator nodes = result.getNodes();
            long count = nodes.getSize();
            
            if (count == -1) {
                count = 0;
                while(nodes.hasNext()) {
                    nodes.nextNode();
                    
                    count++;
                }
            } 
            return count;
        } catch (RepositoryException e) {
            throw new MailboxException("Unable to count unseen messages in mailbox " + mailbox, e);
        }
    }


    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.MessageMapper#delete(org.apache.james.mailbox.store.mail.model.Mailbox, org.apache.james.mailbox.store.mail.model.Message)
     */
    public void delete(Mailbox<String> mailbox, Message<String> message) throws MailboxException {
        JCRMessage membership = (JCRMessage) message;
        if (membership.isPersistent()) {
            try {

                getSession().getNodeByIdentifier(membership.getId()).remove();
            } catch (RepositoryException e) {
                throw new MailboxException("Unable to delete message " + message + " in mailbox " + mailbox, e);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.james.mailbox.store.mail.MessageMapper#findInMailbox(org.apache
     * .james.imap.mailbox.MessageRange)
     */
    public void findInMailbox(Mailbox<String> mailbox, MessageRange set,
    		MailboxMembershipCallback<String> callback) throws MailboxException {
        try {
        	List<Message<String>> results;
            long from = set.getUidFrom();
            final long to = set.getUidTo();
            final int batchSize = set.getBatchSize();
            final Type type = set.getType();
            
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
	                	results = findMessageInMailboxWithUID(mailbox, from);
	                    break;
	                case RANGE:
	                	results = findMessagesInMailboxBetweenUIDs(mailbox, from, to, batchSize);
	                    break;       
	            }
            
	            if(results.size() > 0) {
					callback.onMailboxMembers(results);
										
					// move the start UID behind the last fetched message UID					
					from = results.get(results.size()-1).getUid()+1;
				}
	            
	        } while(results.size() > 0 && batchSize > 0);
        } catch (RepositoryException e) {
            throw new MailboxException("Unable to search MessageRange " + set + " in mailbox " + mailbox, e);
        }
    }
   
    private List<Message<String>> findMessagesInMailboxAfterUID(Mailbox<String> mailbox, long uid, int batchSize) throws RepositoryException {
        List<Message<String>> list = new ArrayList<Message<String>>();
        String queryString = "/jcr:root" + getMailboxPath(mailbox) + "//element(*,jamesMailbox:message)[@" + JCRMessage.UID_PROPERTY + ">=" + uid + "] order by @" + JCRMessage.UID_PROPERTY;

        QueryManager manager = getSession().getWorkspace().getQueryManager();
        Query query = manager.createQuery(queryString, Query.XPATH);
        if(batchSize > 0)
        	query.setLimit(batchSize);
        QueryResult result = query.execute();

        NodeIterator iterator = result.getNodes();
        while (iterator.hasNext()) {
            list.add(new JCRMessage(iterator.nextNode(), getLogger()));
        }
        return list;
    }

    private List<Message<String>> findMessageInMailboxWithUID(Mailbox<String> mailbox, long uid) throws RepositoryException  {
        List<Message<String>> list = new ArrayList<Message<String>>();
        String queryString = "/jcr:root" + getMailboxPath(mailbox) + "//element(*,jamesMailbox:message)[@" + JCRMessage.UID_PROPERTY + "=" + uid + "]";

        QueryManager manager = getSession().getWorkspace().getQueryManager();
        Query query = manager.createQuery(queryString, Query.XPATH);
        query.setLimit(1);
        QueryResult result = query.execute();
        NodeIterator iterator = result.getNodes();
        while (iterator.hasNext()) {
            list.add(new JCRMessage(iterator.nextNode(), getLogger()));
        }
        return list;
    }

    private List<Message<String>> findMessagesInMailboxBetweenUIDs(Mailbox<String> mailbox, long from, long to, int batchSize) throws RepositoryException {
        List<Message<String>> list = new ArrayList<Message<String>>();
        String queryString = "/jcr:root" + getMailboxPath(mailbox) + "//element(*,jamesMailbox:message)[@" + JCRMessage.UID_PROPERTY + ">=" + from + " and @" + JCRMessage.UID_PROPERTY + "<=" + to + "] order by @" + JCRMessage.UID_PROPERTY;
        
        QueryManager manager = getSession().getWorkspace().getQueryManager();
        Query query = manager.createQuery(queryString, Query.XPATH);
        if(batchSize > 0)
        	query.setLimit(batchSize);
        QueryResult result = query.execute();

        NodeIterator iterator = result.getNodes();
        while (iterator.hasNext()) {
            list.add(new JCRMessage(iterator.nextNode(), getLogger()));
        }
        return list;
    }
    
    private List<Message<String>> findMessagesInMailbox(Mailbox<String> mailbox, int batchSize) throws RepositoryException {        
        List<Message<String>> list = new ArrayList<Message<String>>();
        
        String queryString = "/jcr:root" + getMailboxPath(mailbox) + "//element(*,jamesMailbox:message) order by @" + JCRMessage.UID_PROPERTY;
        QueryManager manager = getSession().getWorkspace().getQueryManager();
        Query query = manager.createQuery(queryString, Query.XPATH);
        if(batchSize > 0)
        	query.setLimit(batchSize);
        QueryResult result = query.execute();

        NodeIterator iterator = result.getNodes();
        while (iterator.hasNext()) {
            list.add(new JCRMessage(iterator.nextNode(), getLogger()));
        }
        return list;
    }

    
    
    private List<Message<String>> findDeletedMessagesInMailboxAfterUID(Mailbox<String> mailbox, long uid) throws RepositoryException {
        List<Message<String>> list = new ArrayList<Message<String>>();
        String queryString = "/jcr:root" + getMailboxPath(mailbox) + "//element(*,jamesMailbox:message)[@" + JCRMessage.UID_PROPERTY + ">=" + uid + " and @" + JCRMessage.DELETED_PROPERTY+ "='true'] order by @" + JCRMessage.UID_PROPERTY;
 
        QueryManager manager = getSession().getWorkspace().getQueryManager();
        QueryResult result = manager.createQuery(queryString, Query.XPATH).execute();

        NodeIterator iterator = result.getNodes();
        while (iterator.hasNext()) {
            list.add(new JCRMessage(iterator.nextNode(), getLogger()));
        }
        return list;
    }

    private List<Message<String>> findDeletedMessageInMailboxWithUID(Mailbox<String> mailbox, long uid) throws RepositoryException  {
        List<Message<String>> list = new ArrayList<Message<String>>();
        String queryString = "/jcr:root" + getMailboxPath(mailbox) + "//element(*,jamesMailbox:message)[@" + JCRMessage.UID_PROPERTY + "=" + uid + " and @" + JCRMessage.DELETED_PROPERTY+ "='true']";
        QueryManager manager = getSession().getWorkspace().getQueryManager();
        Query query = manager.createQuery(queryString, Query.XPATH);
        query.setLimit(1);
        QueryResult result = query.execute();

        NodeIterator iterator = result.getNodes();
        while (iterator.hasNext()) {
            JCRMessage member = new JCRMessage(iterator.nextNode(), getLogger());
            list.add(member);
        }
        return list;
    }

    private List<Message<String>> findDeletedMessagesInMailboxBetweenUIDs(Mailbox<String> mailbox, long from, long to) throws RepositoryException {
        List<Message<String>> list = new ArrayList<Message<String>>();
        String queryString = "/jcr:root" + getMailboxPath(mailbox) + "//element(*,jamesMailbox:message)[@" + JCRMessage.UID_PROPERTY + ">=" + from + " and @" + JCRMessage.UID_PROPERTY + "<=" + to + " and @" + JCRMessage.DELETED_PROPERTY+ "='true'] order by @" + JCRMessage.UID_PROPERTY;
       
        QueryManager manager = getSession().getWorkspace().getQueryManager();
        QueryResult result = manager.createQuery(queryString, Query.XPATH).execute();

        NodeIterator iterator = result.getNodes();
        while (iterator.hasNext()) {
            list.add(new JCRMessage(iterator.nextNode(), getLogger()));
        }
        return list;
    }
    
    private List<Message<String>> findDeletedMessagesInMailbox(Mailbox<String> mailbox) throws RepositoryException {
        
        List<Message<String>> list = new ArrayList<Message<String>>();
        String queryString = "/jcr:root" + getMailboxPath(mailbox) + "//element(*,jamesMailbox:message)[@" + JCRMessage.DELETED_PROPERTY+ "='true'] order by @" + JCRMessage.UID_PROPERTY;
        
        QueryManager manager = getSession().getWorkspace().getQueryManager();
        QueryResult result = manager.createQuery(queryString, Query.XPATH).execute();

        NodeIterator iterator = result.getNodes();
        while (iterator.hasNext()) {
            JCRMessage member = new JCRMessage(iterator.nextNode(), getLogger());
            list.add(member);
        }
        return list;
    }


    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.MessageMapper#expungeMarkedForDeletionInMailbox(org.apache.james.mailbox.store.mail.model.Mailbox, org.apache.james.mailbox.MessageRange)
     */
    public Map<Long, MessageMetaData> expungeMarkedForDeletion(Mailbox<String> mailbox, MessageRange set) throws MailboxException {
        try {
            final List<Message<String>> results;
            final long from = set.getUidFrom();
            final long to = set.getUidTo();
            final Type type = set.getType();
            switch (type) {
                default:
                case ALL:
                    results = findDeletedMessagesInMailbox(mailbox);
                    break;
                case FROM:
                    results = findDeletedMessagesInMailboxAfterUID(mailbox, from);
                    break;
                case ONE:
                    results = findDeletedMessageInMailboxWithUID(mailbox, from);
                    break;
                case RANGE:
                    results = findDeletedMessagesInMailboxBetweenUIDs(mailbox, from, to);
                    break;       
            }
            Map<Long, MessageMetaData> uids = new HashMap<Long, MessageMetaData>();
            for (int i = 0; i < results.size(); i++) {
                Message<String> m = results.get(i);
                long uid = m.getUid();
                uids.put(uid, new SimpleMessageMetaData(m));
                delete(mailbox, m);
            }
            return uids;
        } catch (RepositoryException e) {
            throw new MailboxException("Unable to search MessageRange " + set + " in mailbox " + mailbox, e);
        }
    }

    /*
     * 
     * TODO: Maybe we should better use an ItemVisitor and just traverse through the child nodes. This could be a way faster
     * 
     * (non-Javadoc)
     * 
     * @see
     * org.apache.james.mailbox.store.mail.MessageMapper#findRecentMessagesInMailbox
     * ()
     */
    public List<Message<String>> findRecentMessagesInMailbox(Mailbox<String> mailbox) throws MailboxException {
        
        try {
 
            List<Message<String>> list = new ArrayList<Message<String>>();
            String queryString = "/jcr:root" + getMailboxPath(mailbox) + "//element(*,jamesMailbox:message)[@" + JCRMessage.RECENT_PROPERTY +"='true'] order by @" + JCRMessage.UID_PROPERTY;
            
            QueryManager manager = getSession().getWorkspace().getQueryManager();
            Query query = manager.createQuery(queryString, Query.XPATH);
            QueryResult result = query.execute();
            
            NodeIterator iterator = result.getNodes();
            while(iterator.hasNext()) {
                list.add(new JCRMessage(iterator.nextNode(), getLogger()));
            }
            return list;

        } catch (RepositoryException e) {
            throw new MailboxException("Unable to search recent messages in mailbox " + mailbox, e);
        }
    }


    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.MessageMapper#findFirstUnseenMessageUid(org.apache.james.mailbox.store.mail.model.Mailbox)
     */
    public Long findFirstUnseenMessageUid(Mailbox<String> mailbox) throws MailboxException {
        try {
            String queryString = "/jcr:root" + getMailboxPath(mailbox) + "//element(*,jamesMailbox:message)[@" + JCRMessage.SEEN_PROPERTY +"='false'] order by @" + JCRMessage.UID_PROPERTY;

            QueryManager manager = getSession().getWorkspace().getQueryManager();
            
            Query query = manager.createQuery(queryString, Query.XPATH);
            query.setLimit(1);
            QueryResult result = query.execute();

            NodeIterator iterator = result.getNodes();
            if(iterator.hasNext()) {
                return new JCRMessage(iterator.nextNode(), getLogger()).getUid();
            } else {
                return null;
            }
        } catch (RepositoryException e) {
            throw new MailboxException("Unable to find first unseen message in mailbox " + mailbox, e);
        }
    }

    /**
     * Convert the given int value to a String. If the int value is smaller then
     * 9 it will prefix the String with 0.
     * 
     * @param value
     * @return stringValue
     */
    private String convertIntToString(int value) {
        if (value <= 9) {
            return "0" + String.valueOf(value);
        } else {
            return String.valueOf(value);
        }
    }



   



    /**
     * Logout from open JCR Session
     */
    public void endRequest() {
       repository.logout(mSession);
    }
    

    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.AbstractMessageMapper#calculateHigestModSeq(org.apache.james.mailbox.store.mail.model.Mailbox)
     */
    protected long calculateHigestModSeq(Mailbox<String> mailbox) throws MailboxException {
        try {
            Session s = getSession();
            // we use order by because without it count will always be 0 in jackrabbit
            String queryString = "/jcr:root/" + ISO9075.encodePath(s.getNodeByIdentifier(mailbox.getMailboxId()).getPath()) + "//element(*,jamesMailbox:message) order by @" + JCRMessage.MODSEQ_PROPERTY + " descending";
            QueryManager manager = s.getWorkspace().getQueryManager();
            Query q = manager.createQuery(queryString, Query.XPATH);
            q.setLimit(1);
            QueryResult result = q.execute();
            NodeIterator nodes = result.getNodes();
            if (nodes.hasNext()) {
                return nodes.nextNode().getProperty(JCRMessage.UID_PROPERTY).getLong();
            }
            return 0;
        } catch (RepositoryException e) {
            throw new MailboxException("Unable to count unseen messages in mailbox " + mailbox, e);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.AbstractMessageMapper#calculateLastUid(org.apache.james.mailbox.store.mail.model.Mailbox)
     */
    protected long calculateLastUid(Mailbox<String> mailbox) throws MailboxException {
        try {
            Session s = getSession();
            // we use order by because without it count will always be 0 in jackrabbit
            String queryString = "/jcr:root/" + ISO9075.encodePath(s.getNodeByIdentifier(mailbox.getMailboxId()).getPath()) + "//element(*,jamesMailbox:message) order by @" + JCRMessage.UID_PROPERTY + " descending";
            QueryManager manager = s.getWorkspace().getQueryManager();
            Query q = manager.createQuery(queryString, Query.XPATH);
            q.setLimit(1);
            QueryResult result = q.execute();
            NodeIterator nodes = result.getNodes();
            if (nodes.hasNext()) {
                return nodes.nextNode().getProperty(JCRMessage.UID_PROPERTY).getLong();
            }
            return 0;
        } catch (RepositoryException e) {
            throw new MailboxException("Unable to count unseen messages in mailbox " + mailbox, e);
        }
    }

    @Override
    protected void copy(Mailbox<String> mailbox, long uid, long modSeq, Message<String> original) throws MailboxException {
        try {
            String newMessagePath = getSession().getNodeByIdentifier(mailbox.getMailboxId()).getPath() + NODE_DELIMITER + String.valueOf(uid);
            getSession().getWorkspace().copy(((JCRMessage)original).getNode().getPath(), getSession().getNodeByIdentifier(mailbox.getMailboxId()).getPath() + NODE_DELIMITER + String.valueOf(uid));
            Node node = getSession().getNode(newMessagePath);
            node.setProperty(JCRMessage.MAILBOX_UUID_PROPERTY, mailbox.getMailboxId());
            node.setProperty(JCRMessage.UID_PROPERTY, uid);
            node.setProperty(JCRMessage.MODSEQ_PROPERTY, modSeq);

        } catch (RepositoryException e) {
            throw new MailboxException("Unable to copy message " +original + " in mailbox " + mailbox, e);
        }        
    }

    @Override
    protected void save(Mailbox<String> mailbox, Message<String> message) throws MailboxException {
        final JCRMessage membership = (JCRMessage) message;
        try {

            Node messageNode = null;

            if (membership.isPersistent()) {
                messageNode = getSession().getNodeByIdentifier(membership.getId());
            }

            if (messageNode == null) {
               
                Date date = message.getInternalDate();
                if (date == null) {
                    date = new Date();
                }

                // extracte the date from the message to create node structure
                // later
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                final String year = convertIntToString(cal.get(Calendar.YEAR));
                final String month = convertIntToString(cal.get(Calendar.MONTH) + 1);
                final String day = convertIntToString(cal.get(Calendar.DAY_OF_MONTH));
                final String hour = convertIntToString(cal.get(Calendar.HOUR_OF_DAY));
                final String min = convertIntToString(cal.get(Calendar.MINUTE));

                Node mailboxNode = getSession().getNodeByIdentifier(mailbox.getMailboxId());
                Node node = mailboxNode;

                if (scaleType > MESSAGE_SCALE_NONE) {
                    // we lock the whole mailbox with all its childs while
                    // adding the folder structure for the date

                    if (scaleType >= MESSAGE_SCALE_YEAR) {
                        node = JcrUtils.getOrAddFolder(node, year);

                        if (scaleType >= MESSAGE_SCALE_MONTH) {
                            node = JcrUtils.getOrAddFolder(node, month);

                            if (scaleType >= MESSAGE_SCALE_DAY) {
                                node = JcrUtils.getOrAddFolder(node, day);

                                if (scaleType >= MESSAGE_SCALE_HOUR) {
                                    node = JcrUtils.getOrAddFolder(node, hour);

                                    if (scaleType >= MESSAGE_SCALE_MINUTE) {
                                        node = JcrUtils.getOrAddFolder(node, min);
                                    }
                                }
                            }
                        }
                    }

                }

                long uid = membership.getUid();
                messageNode = mailboxNode.addNode(String.valueOf(uid), "nt:file");
                messageNode.addMixin("jamesMailbox:message");
                try {
                    membership.merge(messageNode);

                } catch (IOException e) {
                    throw new RepositoryException("Unable to merge message in to tree", e);
                }                
            } else {
                membership.merge(messageNode);
            }
        } catch (RepositoryException e) {
            throw new MailboxException("Unable to save message " + message + " in mailbox " + mailbox, e);
        } catch (IOException e) {
            throw new MailboxException("Unable to save message " + message + " in mailbox " + mailbox, e);
        }        
    }

    /*
     * (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.AbstractMessageMapper#saveSequences(org.apache.james.mailbox.store.mail.model.Mailbox, long, long)
     */
    protected void saveSequences(Mailbox<String> mailbox, long lastUid, long highestModSeq) throws MailboxException {
        try {
            Node mailboxNode = getSession().getNodeByIdentifier(mailbox.getMailboxId());
            mailboxNode.setProperty(JCRMailbox.HIGHESTKNOWNMODSEQ_PROPERTY, highestModSeq);
            mailboxNode.setProperty(JCRMailbox.LASTKNOWNUID_PROPERTY, lastUid);
           
        } catch (RepositoryException e) {
            throw new MailboxException("Unable to save sequences", e);
        }
    }

}
