package com.paradigma.recommender.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.NoSuchItemException;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoException;

/**
 * <p>
 * A {@link DataModel} backed by a MongoDB database. This class expects a 
 * collection in the database which contains a user ID (<code>long</code> or 
 * <code>{@link ObjectId}</code>), item ID (<code>long</code> or 
 * <code>{@link ObjectId}</code>), preference value (optional) and timestamps 
 * ("created_at", "deleted_at").
 * </p>
 * <p>
 * An example of a document in MongoDB:
 * </p>
 *
 * <p><code>{ "_id" : ObjectId("4d7627bf6c7d47ade9fc7780"), 
 * "user_id" : ObjectId("4c2209fef3924d31102bd84b"), 
 * "item_id" : ObjectId(4c2209fef3924d31202bd853), 
 * "preference" : 0.5, 
 * "created_at" : "Tue Mar 23 2010 20:48:43 GMT-0400 (EDT)" }
 * </code></p>
 *
 * <p>
 * Preference value is optional to accommodate applications that have no notion
 * of a preference value (that is, the user simply expresses a preference for 
 * an item, but no degree of preference).
 * </p>
 *
 * <p>
 * The preference value is assumed to be parseable as a <code>double</code>. 
 * </p>
 * <p>
 * The user IDs and item IDs are assumed to be parseable as <code>long</code>s
 * or <code>{@link ObjectId}</code>s. In case of <code>ObjectId</code>s, the 
 * model creates a HashMap<<code>ObjectId</code>, <code>long</code>> 
 * (collection "mongo_data_model_map") inside the MongoDB database. This 
 * conversion is needed since Mahout uses the long datatype to feed the 
 * recommender, and MongoDB uses 12 bytes to create its identifiers.
 * </p>
 * The timestamps ("created_at", "deleted_at"), if present, are assumed to be 
 * parseable as a <code>long</code> or <code>{@link Date}</code>. To express 
 * timestamps as <code>Date</code>s, a <code>{@link SimpleDateFormat}</code> 
 * must be provided in the class constructor. The default Date format is 
 * <code>"EE MMM dd yyyy HH:mm:ss 'GMT'Z (zzz)"</code>. If this parameter 
 * is set to null, timestamps are assumed to be parseable as <code>long</code>s. 
 * </p>
 *
 * <p>
 * It is also acceptable for the documents to contain additional fields. 
 * Those fields will be ignored.
 * </p>
 *
 * <p>
 * This class will reload data from the MondoDB database when 
 * {@link #refresh(Collection)} is called. MongoDataModel keeps the 
 * timestamp of the last update. This variable and the fields "created_at" 
 * and "deleted_at" help the model to determine if the triple 
 * (user, item, preference) must be added or deleted.
 * </p>
 * 
 * @author Fernando Tapia Rico - fertapric@gmail.com - www.fernandotapiarico.com
 * Paradigma Tecnológico
 */
public final class MongoDataModel implements DataModel {
    
    /**
     * Default MongoDB host. Default: localhost
     */
    protected static String defaultMongoHost = "localhost";
    /**
     * Default MongoDB port. Default: 27017
     */
    protected static int defaultMongoPort = 27017;
    
    /**
     * Default MongoDB database. Default: recommender
     */
    protected static String defaultMongoDB = "recommender";
    
    /**
     * Default MongoDB authentication flag. 
     * Default: false (authentication is not required)
     */
    protected static boolean defaultMongoAuth = false;
    
    /**
     * Default MongoDB user. Default: recommender
     */
    protected static String defaultMongoUsername = "recommender";
    
    /**
     * Default MongoDB password. Default: recommender
     */
    protected static String defaultMongoPassword = "recommender";
    
    /**
     * Default MongoDB table/collection. Default: items
     */
    protected static String defaultMongoCollection = "items";
    
    /**
     * Default MongoDB update flag. When this flag is activated, the
     * DataModel updates both model and database. Default: true
     */
    protected static boolean defaultMongoManage = true;
    
    /**
     * Default MongoDB user ID field. Default: user_id
     */
    protected static String defaultMongoUserID = "user_id";
    
    /**
     * Default MongoDB item ID field. Default: item_id
     */
    protected static String defaultMongoItemID = "item_id";
    
    /**
     * Default MongoDB preference value field. Default: preference
     */
    protected static String defaultMongoPreference = "preference";
    
    /**
     * Default MongoDB final remove flag. Default: false
     */
    protected static boolean defaultMongoFinalRemove = false;
    
    /**
     * Default MongoDB date format. 
     * Default: "EE MMM dd yyyy HH:mm:ss 'GMT'Z (zzz)"
     */
    protected static SimpleDateFormat defaultDateFormat = new SimpleDateFormat("EE MMM dd yyyy HH:mm:ss 'GMT'Z (zzz)");
    
    /**
     * MongoDB host
     */
    protected static String mongoHost = defaultMongoHost;
    
    /**
     * MongoDB port
     */
    protected static int mongoPort = defaultMongoPort;
    
    /**
     * MongoDB database
     */
    protected static String mongoDB = defaultMongoDB;
    
    /**
     * MongoDB authentication flag. If this flag is set to false, 
     * authentication is not required.
     */
    protected static boolean mongoAuth = defaultMongoAuth;
    
    /**
     * MongoDB user
     */
    protected static String mongoUsername = defaultMongoUsername;
    
    /**
     * MongoDB pass
     */
    protected static String mongoPassword = defaultMongoPassword;
    
    /**
     * MongoDB table/collection
     */
    protected static String mongoCollection = defaultMongoCollection;
    
    /**
     * MongoDB update flag. When this flag is activated, the
     * DataModel updates both model and database
     */
    protected static boolean mongoManage = defaultMongoManage;
    
    /**
     * MongoDB user ID field
     */
    protected static String mongoUserID = defaultMongoUserID;
    
    /**
     * MongoDB item ID field
     */
    protected static String mongoItemID = defaultMongoItemID;
    
    /**
     * MongoDB preference value field
     */
    protected static String mongoPreference = defaultMongoPreference;
    
    /**
     * MongoDB final remove flag. Default: false
     */
    protected static boolean mongoFinalRemove = defaultMongoFinalRemove;
    
    /**
     * MongoDB date format
     */
    protected static SimpleDateFormat dateFormat = defaultDateFormat;
    
    protected static String mongoMapCollection = "mongo_data_model_map";
    protected static DBCollection collection;
    protected static DBCollection collectionMap;
    protected static Date mongoTimestamp;
    private final ReentrantLock reloadLock;
    private static DataModel delegate;
    private static boolean userIsObject;
    private static boolean itemIsObject;
    private static boolean preferenceIsString;
    private static long idCounter;
    private static final Logger log = LoggerFactory.getLogger(MongoDataModel.class);
    
    /**
     * Creates a new MongoDataModel
     */
    public MongoDataModel() throws UnknownHostException, MongoException {
        this.reloadLock = new ReentrantLock();
        buildModel();
    }
    
    /**
     * Creates a new MongoDataModel with MongoDB basic configuration 
     * (without authentication)
     * 
     * @param host MongoDB host.
     * @param port MongoDB port. Default: 27017
     * @param database MongoDB database
     * @param collection MongoDB collection/table
     * @param manage If true, the model adds and removes users and items 
     *     from MongoDB database when the model is refreshed.
     * @param finalRemove If true, the model removes the user/item completely 
     *     from the MongoDB database. If false, the model adds the "deleted_at"
     *     field with the current date to the "deleted" user/item.
     * @param format MongoDB date format. If null, the model uses timestamps.
     * 
     * @throws UnknownHostException if the database host cannot be resolved
     * @throws MongoException
     */
    public MongoDataModel(String host, int port, String database,
            String collection, boolean manage, boolean finalRemove,
            SimpleDateFormat format) throws UnknownHostException, 
            MongoException {
        mongoHost = host;
        mongoPort = port;
        mongoDB = database;
        mongoCollection = collection;
        mongoManage = manage;
        mongoFinalRemove = finalRemove;
        dateFormat = format;
        this.reloadLock = new ReentrantLock();
        buildModel();
    }
    
    /**
     * Creates a new MongoDataModel with MongoDB advanced configuration 
     * (without authentication)
     * 
     * @param userIDField Mongo user ID field
     * @param itemIDField Mongo item ID field
     * @param preferenceField Mongo preference value field
     * 
     * @throws UnknownHostException if the database host cannot be resolved
     * @throws MongoException
     * 
     * @see #MongoDataModel(String, int, String, String, boolean, boolean, SimpleDateFormat)
     */
    public MongoDataModel(String host, int port, String database,
            String collection, boolean manage, boolean finalRemove,
            SimpleDateFormat format, String userIDField, String itemIDField,
            String preferenceField) throws UnknownHostException, MongoException {
        mongoHost = host;
        mongoPort = port;
        mongoDB = database;
        mongoCollection = collection;
        mongoManage = manage;
        mongoFinalRemove = finalRemove;
        dateFormat = format;
        mongoUserID = userIDField;
        mongoItemID = itemIDField;
        mongoPreference = preferenceField;
        this.reloadLock = new ReentrantLock();
        buildModel();
    }
    
    /**
     * Creates a new MongoDataModel with MongoDB basic configuration 
     * (with authentication)
     * 
     * @param user Mongo username (authentication)
     * @param password Mongo password (authentication)
     * 
     * @throws UnknownHostException if the database host cannot be resolved
     * @throws MongoException
     * 
     * @see #MongoDataModel(String, int, String, String, boolean, boolean, SimpleDateFormat)
     */
    public MongoDataModel(String host, int port, String database,
            String collection, boolean manage, boolean finalRemove,
            SimpleDateFormat format,String user,String password) 
            throws UnknownHostException, MongoException {
        mongoHost = host;
        mongoPort = port;
        mongoDB = database;
        mongoCollection = collection;
        mongoManage = manage;
        mongoFinalRemove = finalRemove;
        dateFormat = format;
        mongoAuth = true;
        mongoUsername = user;
        mongoPassword = password;
        this.reloadLock = new ReentrantLock();
        buildModel();
    }
    
    /**
     * Creates a new MongoDataModel with MongoDB advanced configuration 
     * (with authentication)
     * 
     * @throws UnknownHostException if the database host cannot be resolved
     * @throws MongoException
     * 
     * @see #MongoDataModel(String, int, String, String, boolean, boolean, SimpleDateFormat, String, String, String)
     * @see #MongoDataModel(String, int, String, String, boolean, boolean, SimpleDateFormat, String, String)
     */
    public MongoDataModel(String host, int port, String database,
            String collection, boolean manage, boolean finalRemove,
            SimpleDateFormat format, String user, String password,
            String userIDField, String itemIDField, String preferenceField)
            throws UnknownHostException, MongoException {
        mongoHost = host;
        mongoPort = port;
        mongoDB = database;
        mongoCollection = collection;
        mongoManage = manage;
        mongoFinalRemove = finalRemove;
        dateFormat = format;
        mongoAuth = true;
        mongoUsername = user;
        mongoPassword = password;
        mongoUserID = userIDField;
        mongoItemID = itemIDField;
        mongoPreference = preferenceField;
        this.reloadLock = new ReentrantLock();
        buildModel();
    }
    
    /**
     * <p>
     * Adds/removes (user, item) pairs to/from the model.
     * </p>
     *
     * @param userID MongoDB user identifier
     * @param items List of pairs (item, preference) which want to be added or 
     *     deleted
     * @param add If true, this flag indicates that the    pairs (user, item) 
     *     must be added to the model. If false, it indicates deletion.
     * 
     * @throws NullPointerException
     * @throws IllegalArgumentException
     * @throws NoSuchUserException
     * @throws NoSuchItemException
     * 
     * 
     * @see #refresh(Collection)
     */
    public void refreshData(String userID, ArrayList<ArrayList<String>> items, 
            boolean add) throws NullPointerException, NoSuchUserException, 
            NoSuchItemException, IllegalArgumentException {
        checkData(userID, items, add);
        long id = Long.parseLong(fromIdToLong(userID, true));
        for (ArrayList<String> item : items) {
            item.set(0, fromIdToLong(item.get(0), false));
        }
        if (reloadLock.tryLock()) {
            try {
                if (add) {
                    delegate = addUserItem(id, items);
                } else {
                    delegate = removeUserItem(id, items);
                }
            } catch (Exception ioe) {
                log.warn("Exception while reloading", ioe);
            } finally {
                reloadLock.unlock();
            }
        }
    }
    
    
    /**
     * <p>
     * Triggers "refresh" -- whatever that means -- of the implementation. 
     * The general contract is that any should always leave itself in a 
     * consistent, operational state, and that the refresh atomically updates 
     * internal state from old to new.
     * </p>
     *
     * @param alreadyRefreshed 
     *     s that are known to have already been refreshed as 
     *     a result of an initial call to a method on some object. This ensures 
     *     that objects in a refresh dependency graph aren't refreshed twice 
     *     needlessly.
     * 
     * @throws NullPointerException
     * @throws IllegalArgumentException
     * 
     * @see #refreshData(String, ArrayList, boolean)
     */
    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) 
            throws NullPointerException, IllegalArgumentException {
        Date ts = new Date(0);
        BasicDBObject query = new BasicDBObject();
        query.put("deleted_at", new BasicDBObject("$gt", mongoTimestamp));
        DBCursor cursor = collection.find(query);
        while(cursor.hasNext()) {
            HashMap<String, Object> user = (HashMap<String, Object>) cursor.next().toMap();
            String userID = getID(user.get(mongoUserID), true);
            ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
            ArrayList<String> item = new ArrayList<String>();
            item.add(getID(user.get(mongoItemID), false));
            item.add(Float.toString(getPreference(user.get(mongoPreference))));
            items.add(item);
            try {
                refreshData(userID, items, false);
            } catch (NoSuchUserException e) {
                log.error("NoSuchUserException.    User ID: " + userID);
            } catch (NoSuchItemException e) {
                log.error("NoSuchItemException.    Item ID: " + items);
            }
            if (ts.compareTo(getDate(user.get("created_at"))) < 0) {
                ts = getDate(user.get("created_at"));
            }
        }
        query = new BasicDBObject();
        query.put("created_at", new BasicDBObject("$gt", mongoTimestamp));
        cursor = collection.find(query);
        while(cursor.hasNext()) {
            HashMap<String, Object> user = (HashMap<String, Object>) cursor.next().toMap();
            if (!user.containsKey("deleted_at")) {
                String userID = getID(user.get(mongoUserID), true);
                ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
                ArrayList<String> item = new ArrayList<String>();
                item.add(getID(user.get(mongoItemID), false));
                item.add(Float.toString(getPreference(user.get(mongoPreference))));
                items.add(item);
                try {
                    refreshData(userID, items, true);
                } catch (NoSuchUserException e) {
                    log.error("NoSuchUserException.    User ID: " + userID);
                } catch (NoSuchItemException e) {
                    log.error("NoSuchItemException.    Items: " + items);
                }
                if (ts.compareTo(getDate(user.get("created_at"))) < 0) {
                    ts = getDate(user.get("created_at"));
                }
            }
        }
        if (mongoTimestamp.compareTo(ts) < 0) {
            mongoTimestamp = ts;
        }
    }
    
    /**
     * <p>
     * Translates the MongoDB identifier to Mahout/MongoDataModel's internal 
     * identifier, if required.
     * </p>
     * <p>
     * If MongoDB identifiers are long datatypes, it returns the id.
     * </p>
     * <p>
     * This conversion is needed since Mahout uses the long datatype to feed the
     * recommender, and MongoDB uses 12 bytes to create its identifiers.
     * </p>
     * 
     * @param id MongoDB identifier
     * @param isUser
     * 
     * @return String containing the translation of the external MongoDB ID to 
     * internal long ID (mapping).
     * 
     * @see #fromLongToId(long)
     * @see <a href="http://www.mongodb.org/display/DOCS/Object%20IDs">
     *     Mongo Object IDs</a>
     */
    public String fromIdToLong(String id, boolean isUser) {
        DBObject objectIdLong = collectionMap.findOne(new BasicDBObject("element_id", id));
        if (objectIdLong != null) {
            HashMap<String, Object> idLong = (HashMap<String, Object>) objectIdLong.toMap();
            return (String) idLong.get("long_value");
        } else {
            objectIdLong = new BasicDBObject();
            String longValue = Long.toString(idCounter++);
            objectIdLong.put("element_id", id);
            objectIdLong.put("long_value", longValue);
            collectionMap.insert(objectIdLong);
            log.info("[+++][MONGO-MAP] Adding Translation    "
                + (isUser ? "User ID" : "Item ID" ) + ": " + id 
                + " long_value: " + longValue);
            return longValue;
        }
    }
    
    /**
     * <p>
     * Translates the Mahout/MongoDataModel's internal identifier to MongoDB 
     * identifier, if required.
     * </p>
     * <p>
     * If MongoDB identifiers are long datatypes, it returns the id in String 
     * format.
     * </p>
     * <p>
     * This conversion is needed since Mahout uses the long datatype to feed the
     * recommender, and MongoDB uses 12 bytes to create its identifiers.
     * </p>
     *
     * @param id Mahout's internal identifier
     * 
     * @return String containing the translation of the internal long ID to 
     * external MongoDB ID (mapping).
     * 
     * @see #fromIdToLong(String, boolean)
     * @see <a href="http://www.mongodb.org/display/DOCS/Object%20IDs">
     *     Mongo Object IDs</a>
     */
    public String fromLongToId(long id) {
        DBObject objectIdLong = collectionMap.findOne(new BasicDBObject("long_value", Long.toString(id)));
        HashMap<String, Object> idLong = (HashMap<String, Object>) objectIdLong.toMap();
        return (String) idLong.get("element_id");
    }
    /**
     * <p>
     * Checks if an ID is currently in the model.
     * </p>
     * 
     * @return true: if ID is into the model; false: if it's not.
     *
     * @param ID user or item ID
     */
    public boolean isIDInModel(String ID) {
        DBObject objectIdLong = collectionMap.findOne(new BasicDBObject("element_id", ID));
        if (objectIdLong == null) return false;
        return true;
    }
    /**
     * <p>
     * Date of the latest update of the model.
     * </p>
     * 
     * @return Date with the latest update of the model.
     */
    public Date mongoUpdateDate() {
        return mongoTimestamp;
    }
    
    private void buildModel() throws UnknownHostException, MongoException {
        userIsObject = false;
        itemIsObject = false;
        idCounter = 0;
        preferenceIsString = true;
        Mongo mongoDDBB = new Mongo(mongoHost , mongoPort);
        DB db = mongoDDBB.getDB(mongoDB);
        mongoTimestamp = new Date(0);
        FastByIDMap<Collection<Preference>> userIDPrefMap = new FastByIDMap<Collection<Preference>>();
        if (!mongoAuth || (mongoAuth && db.authenticate(mongoUsername, mongoPassword.toCharArray()))) {
            collection = db.getCollection(mongoCollection);
            collectionMap = db.getCollection(mongoMapCollection);
            collectionMap.remove(new BasicDBObject());
            DBCursor cursor = collection.find();
            while(cursor.hasNext()) {
                HashMap<String, Object> user = (HashMap<String, Object>) cursor.next().toMap();
                if (!user.containsKey("deleted_at")) {
                    long userID = Long.parseLong(fromIdToLong(getID(user.get(mongoUserID), true), true));
                    long itemID = Long.parseLong(fromIdToLong(getID(user.get(mongoItemID), false), false));
                    float ratingValue = getPreference(user.get(mongoPreference));
                    Collection<Preference> userPrefs = userIDPrefMap.get(userID);
                    if ( userPrefs == null ) {
                        userPrefs = new ArrayList<Preference>(2);
                        userIDPrefMap.put(userID, userPrefs);
                    }
                    userPrefs.add(new GenericPreference(userID, itemID, ratingValue));
                    if (user.containsKey("created_at") && 
                            mongoTimestamp.compareTo(getDate(user.get("created_at"))) < 0) {
                        mongoTimestamp = getDate(user.get("created_at"));
                    }
                }
            }
        }
        delegate = new GenericDataModel(GenericDataModel.toDataMap(userIDPrefMap, true));
    }
    
    private void removeMongoUserItem(String userID, String itemID) {
        String userId = fromLongToId(Long.parseLong(userID));
        String itemId = fromLongToId(Long.parseLong(itemID));
        if (isUserItemInDB(userId, itemId)) {
            mongoTimestamp = new Date();
            BasicDBObject query = new BasicDBObject();
            query.put(mongoUserID, (userIsObject ? new ObjectId(userId) : userId));
            query.put(mongoItemID, (itemIsObject ? new ObjectId(itemId) : itemId));
            if (mongoFinalRemove) {
                log.info(collection.remove(query).toString());
            } else {
                BasicDBObject update = new BasicDBObject();
                update.put("$set", new BasicDBObject("deleted_at", mongoTimestamp));
                log.info(collection.update(query, update).toString());
            }
            log.info("[---][MONGO] Removing userID: " + userId + " itemID: " + itemId);
        }
    }
    
    private void addMongoUserItem(String userID, String itemID, String preferenceValue) {
        String userId = fromLongToId(Long.parseLong(userID));
        String itemId = fromLongToId(Long.parseLong(itemID));
        if (!isUserItemInDB(userId, itemId)) {
            mongoTimestamp = new Date();
            BasicDBObject user = new BasicDBObject();
            Object userIdObject = (userIsObject ? new ObjectId(userId) : userId);
            Object itemIdObject = (itemIsObject ? new ObjectId(itemId) : itemId);
            user.put(mongoUserID, userIdObject);
            user.put(mongoItemID, itemIdObject);
            user.put(mongoPreference, (preferenceIsString ? preferenceValue : Double.parseDouble(preferenceValue)));
            user.put("created_at", mongoTimestamp);
            collection.insert(user);
            log.info("[+++][MONGO] Adding userID: " + userId + 
                             " itemID: " + itemId + " preferenceValue: " + preferenceValue);
        }
    }
    
    private boolean isUserItemInDB(String userID, String itemID) {
        BasicDBObject query = new BasicDBObject();
        Object userId = (userIsObject ? new ObjectId(userID) : userID);
        Object itemId = (itemIsObject ? new ObjectId(itemID) : itemID);
        query.put(mongoUserID, userId);
        query.put(mongoItemID, itemId);
        if (collection.findOne(query) != null) {
            return true;
        } else {
            return false;
        }
    }
    
    private DataModel removeUserItem(long userID, ArrayList<ArrayList<String>> items) {
        FastByIDMap<PreferenceArray> rawData = ((GenericDataModel) delegate).getRawUserData();
        for (ArrayList<String> item : items) {
            PreferenceArray prefs = (PreferenceArray) rawData.get(userID);
            long itemID = Long.parseLong(item.get(0));
            if (prefs != null) {
                boolean exists = false;
                int length = prefs.length();
                for (int i = 0; i < length; i++) {
                    if (prefs.getItemID(i) == itemID) {
                        exists = true;
                        break;
                    }
                }
                if (exists) {
                    rawData.remove(userID);
                    if (length > 1) {
                        PreferenceArray newPrefs = new GenericUserPreferenceArray(length - 1);
                        for (int i = 0, j = 0; i < length; i++, j++) {
                            if (prefs.getItemID(i) == itemID) {
                                j--;
                            } else {
                                newPrefs.set(j, prefs.get(i));
                            }
                        }
                        ((FastByIDMap<PreferenceArray>) rawData).put(userID, newPrefs);
                    }
                    log.info("[---][MODEL] Removing userID: " + userID + " itemID: " + itemID);
                    if (mongoManage) {
                        removeMongoUserItem(Long.toString(userID), Long.toString(itemID));
                    }
                }
            }
        }
        return new GenericDataModel(rawData);
    }
    
    private DataModel addUserItem(long userID,ArrayList<ArrayList<String>> items) throws Exception {
        FastByIDMap<PreferenceArray> rawData = ((GenericDataModel) delegate).getRawUserData();
        PreferenceArray prefs = (PreferenceArray) rawData.get(userID);
        for (ArrayList<String> item : items) {
            long itemID = Long.parseLong(item.get(0));
            float preferenceValue = Float.parseFloat(item.get(1));
            boolean exists = false;
            if (prefs != null) {
                for (int i = 0; i < prefs.length(); i++) {
                    if (prefs.getItemID(i) == itemID) {
                        exists = true;
                        prefs.setValue(i, preferenceValue);
                        break;
                    }
                }
            }
            if (!exists) {
                if (prefs == null) {
                    prefs = new GenericUserPreferenceArray(1);
                } else {
                    PreferenceArray newPrefs = new GenericUserPreferenceArray(prefs.length() + 1);
                    for (int i = 0, j = 1; i < prefs.length(); i++, j++) {
                        newPrefs.set(j, prefs.get(i));
                    }
                    prefs = newPrefs;
                }
                prefs.setUserID(0, userID);
                prefs.setItemID(0, itemID);
                prefs.setValue(0, preferenceValue);
                log.info("[+++][MODEL] Adding userID: " + userID + 
                                 " itemID: " + itemID + " preferenceValue: " + preferenceValue);
                ((FastByIDMap<PreferenceArray>) rawData).put(userID, prefs);
                if (mongoManage) {
                    addMongoUserItem(Long.toString(userID),
                                                     Long.toString(itemID),
                                                     Float.toString(preferenceValue));
                }
            }
        }
        return new GenericDataModel(rawData);
    }
    
    private Date getDate(Object date) {
        if (date.getClass().getName().contains("Date")) {
            return (Date) date;
        } else if (date.getClass().getName().contains("String")) {
            try {
                return dateFormat.parse((String) date);
            } catch (Exception ioe) {
                log.warn("Error parsing timestamp", ioe);
            }
        }
        return new Date(0);
    }
    
    private float getPreference(Object value) {
        if (value != null) {
            if (value.getClass().getName().contains("String")) {
                preferenceIsString = true;
                return Float.parseFloat((String) value);
            } else {
                preferenceIsString = false;
                return Double.valueOf(value.toString()).floatValue();
            }
        } else {
            return (float) 0.5;
        }
    }
    
    private String getID(Object id, boolean isUser) {
        if (id.getClass().getName().contains("ObjectId")) {
            if (isUser) {
                userIsObject = true;
            } else {
                itemIsObject = true;
            }
            return ((ObjectId) id).toStringMongod();
        } else {
            return (String) id;
        }
    }
    
    private void checkData(String userID, ArrayList<ArrayList<String>> items, 
            boolean add) throws NullPointerException, NoSuchUserException, 
            NoSuchItemException, IllegalArgumentException {
        /* NullPointerExceptions */
        if (userID == null) {
            throw new NullPointerException();
        }
        if (items == null) {
                throw new NullPointerException();
        } else {
            for (ArrayList<String> item : items) {
                if (item.get(0) == null) {
                    throw new NullPointerException();
                }
            }
        }
        /* IllegalArgumentException */
        if (userID == "" || (userIsObject && !userID.matches("[a-f0-9]{24}"))) {
            throw new IllegalArgumentException();
        }
        for (ArrayList<String> item : items) {
            if (item.get(0) == "" ||    (itemIsObject && !item.get(0).matches("[a-f0-9]{24}"))) {
                throw new IllegalArgumentException();
            }
        }
        /* NoSuchUserException */
        if (!add && !isIDInModel(userID)) {
            throw new NoSuchUserException();
        }
        /* NoSuchItemException */
        for (ArrayList<String> item : items) {
            if (!add && !isIDInModel(userID)) {
                throw new NoSuchItemException();
            }
        }
    }
    
    @Override
    public LongPrimitiveIterator getUserIDs() throws TasteException {
        return delegate.getUserIDs();
    }
    
    @Override
    public PreferenceArray getPreferencesFromUser(long id) throws TasteException {
        return delegate.getPreferencesFromUser(id);
    }
    
    @Override
    public FastIDSet getItemIDsFromUser(long userID) throws TasteException {
        return delegate.getItemIDsFromUser(userID);
    }
    
    @Override
    public LongPrimitiveIterator getItemIDs() throws TasteException {
        return delegate.getItemIDs();
    }
    
    @Override
    public PreferenceArray getPreferencesForItem(long itemID) throws TasteException {
        return delegate.getPreferencesForItem(itemID);
    }
    
    @Override
    public Float getPreferenceValue(long userID, long itemID) throws TasteException {
        return delegate.getPreferenceValue(userID, itemID);
    }

    @Override
    public Long getPreferenceTime(long userID, long itemID) throws TasteException {
        return delegate.getPreferenceTime(userID, itemID);
    }

    @Override
    public int getNumItems() throws TasteException {
        return delegate.getNumItems();
    }
    
    @Override
    public int getNumUsers() throws TasteException {
        return delegate.getNumUsers();
    }
    
    @Override
    public int getNumUsersWithPreferenceFor(long... itemIDs) throws TasteException {
        return delegate.getNumUsersWithPreferenceFor(itemIDs);
    }
    
    @Override
    public void setPreference(long userID, long itemID, float value) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void removePreference(long userID, long itemID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasPreferenceValues() {
        return delegate.hasPreferenceValues();
    }

    @Override
    public float getMaxPreference() {
        return delegate.getMaxPreference();
    }

    @Override
    public float getMinPreference() {
        return delegate.getMinPreference();
    }
    
    @Override
    public String toString() {
        return "MongoDataModel";
    }
    
}
