import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.paradigma.recommender.db.MongoDataModel;
import com.paradigma.recommender.GeneralRecommender;


/** 
* @author Fernando Tapia Rico
*/
public class TestStarter {
  

  /**
   * Configuration file
   */
  private static final String RECOMMENDER_PROPERTIES = "./resources/recommender.properties";
  
  /**
   * Configuration file
   */
  private static final String LOG4J_PROPERTIES = "./resources/log4j.properties";

  // Logger
  private static final Logger log = LoggerFactory.getLogger(TestStarter.class);
  
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
   * Default MongoDB authentication flag. Default: false (authentication is not required)
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
   * Default MongoDB date format. Default: "EE MMM dd yyyy HH:mm:ss 'GMT'Z (zzz)"
   */
  protected static SimpleDateFormat defaultDateFormat = new SimpleDateFormat("EE MMM dd yyyy HH:mm:ss 'GMT'Z (zzz)");
  
  /**
   * Default user threshold
   */
  protected static double defaultUserThreshold = 0.8;
  
  /**
   * Default number of Neighbors
   */
  protected static int defaultNeighborsNumber = 10;
   
  /**
   * Default maximum number of recommendations
   */
  protected static int defaultMaxRecommendations = 10;
   
  /**
   * Default user similarity method
   */
  protected static String defaultSimilarityMeasure = "euclidean";
   
  /**
   * Default neighborhood type
   */
  protected static String defaultNeighborhoodType = "nearest";
  
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
   * MongoDB authentication flag. If this flag is set to false, authentication is not required.
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
  
  /**
   * User threshold
   */
  protected static double userThreshold = defaultUserThreshold;

  /**
   * Number of Neighbors
   */
   protected static int neighborsNumber = defaultNeighborsNumber;

  /**
   * Maximum number of recommendations
   */
  protected static int maxRecommendations = defaultMaxRecommendations;
  
  /**
   * Neighborhood type
   */
  protected static String neighborhoodType = defaultNeighborhoodType;
  
  /**
   * User similarity method
   */
  protected static String similarityMeasure = defaultSimilarityMeasure;

  protected static GeneralRecommender recommender;
  protected static boolean userIsObject = true;
  protected static boolean itemIsObject = true;
  protected static boolean preferenceIsString = true;

  public static void main (String args[]) {
    // Starts service
    try {
      log.info("Starting Tastenet item recommender server");
      getParameters();
      recommender = new GeneralRecommender();
      if (mongoAuth) {
        recommender.start(new MongoDataModel(mongoHost,
                                             mongoPort,
                                             mongoDB,
                                             mongoCollection,
                                             mongoManage,
                                             mongoFinalRemove,
                                             dateFormat,
                                             mongoUsername,
                                             mongoPassword,
                                             mongoUserID,
                                             mongoItemID,
                                             mongoPreference),
                          userThreshold,
                          neighborsNumber,
                          maxRecommendations,
                          similarityMeasure,
                          neighborhoodType);
      } else {
        recommender.start(new MongoDataModel(mongoHost,
                                             mongoPort,
                                             mongoDB,
                                             mongoCollection,
                                             mongoManage,
                                             mongoFinalRemove,
                                             dateFormat,
                                             mongoUserID,
                                             mongoItemID,
                                             mongoPreference),
                          userThreshold,
                          neighborsNumber,
                          maxRecommendations,
                          similarityMeasure,
                          neighborhoodType);
      }
      userLoop();
    } catch (Exception e) {
      log.error("Error while starting service.", e);
    }
  }
  
  private static void userLoop() throws Exception{
    BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
    while(true) {
      boolean isMethodSet = false;
      int method = 0;
      while(!isMethodSet) {
        System.out.println("Select method:");
        System.out.println(" [1] Recommend users");
        System.out.println(" [2] Recommend items");
        System.out.println(" [3] Add user/items");
        System.out.println(" [4] Remove user/items");
        System.out.println(" [5] Refresh model");
        System.out.println(" [6] Insert test data in mongoDB");
        System.out.println(" [7] Print users in the model");
        System.out.println(" [8] Print items in the model");
        System.out.println(" [9] Print items by user");
        System.out.println(" [10] Get mongoDB timestamp update");
        String methodID = console.readLine();
        try {
          method = Integer.parseInt(methodID);
          isMethodSet = true;
        } catch(Exception e) {
          System.out.println("Method not valid");
        }
      }
      String userID = "0";
      if (method < 5 || method == 9) {
        System.out.println("Introduce user:");
        System.out.println(" [long] 12332423");
        System.out.println(" [id (string)] 12b378c1212");
        System.out.println(" [id (ObjectId)] ObjectId(\"12b378c1212\")");
        userID = console.readLine();
      }
      boolean wantsItems = false;
      switch(method) {
        case 1:
        case 2:
          wantsItems = askForWantsItems(console);
          if (wantsItems) {
          
          } else {
            ArrayList<String> recommendation = null;
            if (method == 1) {
              System.out.println("Recommending users...");
              recommendation = recommender.recommend(userID, null, true);
            } else {
              System.out.println("Recommending items...");
              recommendation = recommender.recommend(userID, null, false);
            }
            System.out.println(recommendation);
          }
          break;
        case 3:
        case 4:
          ArrayList<ArrayList<String>> items = askForItems(console);
          System.out.println((method == 3 ? "Adding " : "Removing ") + " user: " + userID + "   Items: " + items);
          recommender.refreshData(userID, items, (method == 3 ? true : false));
          break;
        case 5:
          recommender.refresh(null);
          break;
        case 6:
          clean();
          insertTestData();
          // Reiniciando
          System.out.println("Generando de nuevo el recomendador: ");
          main(null);
          break;
        case 7:
          LongPrimitiveIterator userIDs = ((MongoDataModel) recommender.getDataModel()).getUserIDs();
          while(userIDs.hasNext()) {
            System.out.println("User ID: " + 
            ((MongoDataModel) recommender.getDataModel()).fromLongToId(userIDs.nextLong()));
          }
          break;
        case 8:
            LongPrimitiveIterator itemIDs = ((MongoDataModel) recommender.getDataModel()).getItemIDs();
            while(itemIDs.hasNext()) {
              System.out.println("Item ID: " + 
              ((MongoDataModel) recommender.getDataModel()).fromLongToId(itemIDs.nextLong()));
            }
            break;
        case 9:
            long userIDid = Long.parseLong(((MongoDataModel) recommender.getDataModel()).fromIdToLong(userID, true));
            LongPrimitiveIterator itemsByUserIDs = ((MongoDataModel) recommender.getDataModel()).getItemIDsFromUser(userIDid).iterator();
            while(itemsByUserIDs.hasNext()) {
              System.out.println("Item by user:" + userID +" ID: " + 
              ((MongoDataModel) recommender.getDataModel()).fromLongToId(itemsByUserIDs.nextLong()));
            }
            break;
        case 10:
          System.out.println(((MongoDataModel) recommender.getDataModel()).mongoUpdateDate());
          break;
      }
    }
  }

  private static boolean askForWantsItems(BufferedReader console) throws Exception{
    boolean wantsItems = true;
    boolean wantsItemSet = false;
    while(!wantsItemSet) {
      System.out.println("Do you want to provide items? (true, false):");
      String wantsItemsBool = console.readLine();
      try {
        wantsItems = Boolean.parseBoolean(wantsItemsBool);
        wantsItemSet = true;
      } catch(Exception e) {
        System.out.println("Value not valid");
      }
    }
    return wantsItems;
  }
  
  private static ArrayList<ArrayList<String>> askForItems(BufferedReader console) throws Exception{
    boolean everythingOK = false;
    ArrayList<ArrayList<String>> itemsOut = new ArrayList<ArrayList<String>>();
    while(!everythingOK) {
        System.out.println("Introduce items (itemID,preference;itemID,preference;itemID,preference):");
        String itemsString = console.readLine();
        try {
          String[] items = itemsString.split(";");
          for (String item : items) {
            String[] data = item.split(",");
            ArrayList<String> itemOut = new ArrayList<String>();
            itemOut.add(data[0].trim());
            itemOut.add(data[1].trim());
            itemsOut.add(itemOut);
          }
          everythingOK = true;
        } catch(Exception e) {
          System.out.println("Items not valid");
        }
      }
    return itemsOut;
  }
  
  private static void getParameters() {
    try {
      log.info("Reading parameters");
      Properties properties = new Properties();
      properties.load(new FileInputStream(RECOMMENDER_PROPERTIES));
      PropertyConfigurator.configure(new File(LOG4J_PROPERTIES).getAbsolutePath());
      
      String mongoHostProperty = properties.getProperty("MONGO_HOST");
      if (mongoHostProperty != null && mongoHostProperty.length() > 0) {
        try {
          mongoHost = mongoHostProperty;
        } catch (Exception e) {
          log.error("Property [MONGO_HOST] on properties file has an invalid value("
          + mongoHostProperty + ")");
          System.exit(0);
        }
      }
      log.info("MONGO_HOST read from configuration file: " + mongoHost);
      
      String mongoPortProperty = properties.getProperty("MONGO_PORT");
      if (mongoPortProperty != null && mongoPortProperty.length() > 0) {
        try {
          mongoPort = Integer.parseInt(mongoPortProperty);
        } catch (Exception e) {
          log.error("Property [MONGO_PORT] on properties file has an invalid value("
          + mongoPortProperty + ")");
          System.exit(0);
        }
      }
      log.info("MONGO_PORT read from configuration file: " + mongoPort);
      
      String mongoDBProperty = properties.getProperty("MONGO_DB");
      if (mongoDBProperty != null && mongoDBProperty.length() > 0) {
        try {
          mongoDB = mongoDBProperty;
        } catch (Exception e) {
          log.error("Property [MONGO_DB] on properties file has an invalid value("
          + mongoDBProperty + ")");
          System.exit(0);
        }
      }
      log.info("MONGO_DB read from configuration file: " + mongoDB);
      
      String mongoCollectionProperty = properties.getProperty("MONGO_COLLECTION");
      if (mongoCollectionProperty != null && mongoCollectionProperty.length() > 0) {
        try {
          mongoCollection = mongoCollectionProperty;
        } catch (Exception e) {
          log.error("Property [MONGO_COLLECTION] on properties file has an invalid value("
          + mongoCollectionProperty + ")");
          System.exit(0);
        }
      }
      log.info("MONGO_COLLECTION read from configuration file: " + mongoCollection);
      
      String mongoManageProperty = properties.getProperty("MONGO_MANAGE");
      if (mongoManageProperty != null && mongoManageProperty.length() > 0) {
        try {
          mongoManage = Boolean.parseBoolean(mongoManageProperty);
        } catch (Exception e) {
          log.error("Property [MONGO_MANAGE] on properties file has an invalid value("
          + mongoManageProperty + ")");
          System.exit(0);
        }
      }
      log.info("MONGO_MANAGE read from configuration file: " + mongoManage);
      
      String mongoFinalRemoveProperty = properties.getProperty("MONGO_FINAL_REMOVE");
      if (mongoFinalRemoveProperty != null && mongoFinalRemoveProperty.length() > 0) {
        try {
          mongoFinalRemove = Boolean.parseBoolean(mongoFinalRemoveProperty);
        } catch (Exception e) {
          log.error("Property [MONGO_FINAL_REMOVE] on properties file has an invalid value("
          + mongoFinalRemoveProperty + ")");
          System.exit(0);
        }
      }
      log.info("MONGO_FINAL_REMOVE read from configuration file: " + mongoFinalRemove);
      
      String dateFormatProperty = properties.getProperty("DATE_FORMAT");
      if (dateFormatProperty != null && dateFormatProperty.length() > 0) {
        try {
          if (dateFormatProperty.equals("NULL")) {
            dateFormat = null;
          } else {
            dateFormat = new SimpleDateFormat(dateFormatProperty);
          }
        } catch (Exception e) {
          log.error("Property [DATE_FORMAT] on properties file has an invalid value("
          + dateFormatProperty + ")");
          System.exit(0);
        }
      }
      log.info("DATE_FORMAT read from configuration file: " + (dateFormat == null ? "NULL" : dateFormat.toPattern()));
      
      String mongoAuthProperty = properties.getProperty("MONGO_AUTH");
      if (mongoAuthProperty != null && mongoAuthProperty.length() > 0) {
        try {
          mongoAuth = Boolean.parseBoolean(mongoAuthProperty);
        } catch (Exception e) {
          log.error("Property [MONGO_AUTH] on properties file has an invalid value("
          + mongoAuthProperty + ")");
          System.exit(0);
        }
      }
      log.info("MONGO_AUTH read from configuration file: " + mongoAuth);
      
      if (mongoAuth) {
        String mongoUsernameProperty = properties.getProperty("MONGO_USERNAME");
        if (mongoUsernameProperty != null && mongoUsernameProperty.length() > 0) {
          try {
            mongoUsername = mongoUsernameProperty;
          } catch (Exception e) {
            log.error("Property [MONGO_USERNAME] on properties file has an invalid value("
            + mongoUsernameProperty + ")");
            System.exit(0);
          }
        }
        log.info("MONGO_USERNAME read from configuration file: " + mongoUsername);
        
        String mongoPasswordProperty = properties.getProperty("MONGO_PASSWORD");
        if (mongoPasswordProperty != null && mongoPasswordProperty.length() > 0) {
          try {
            mongoPassword = mongoPasswordProperty;
          } catch (Exception e) {
            log.error("Property [MONGO_PASSWORD] on properties file has an invalid value("
            + mongoPasswordProperty + ")");
            System.exit(0);
          }
        }
        log.info("MONGO_PASSWORD read from configuration file: " + mongoPassword);
      }
      
      String mongoUserIDProperty = properties.getProperty("MONGO_USER_FIELD");
      if (mongoUserIDProperty != null && mongoUserIDProperty.length() > 0) {
        try {
          mongoUserID = mongoUserIDProperty;
        } catch (Exception e) {
          log.error("Property [MONGO_USER_FIELD] on properties file has an invalid value("
          + mongoUserIDProperty + ")");
          System.exit(0);
        }
      }
      log.info("MONGO_USER_FIELD read from configuration file: " + mongoUserID);
      
      String mongoItemIDProperty = properties.getProperty("MONGO_ITEM_FIELD");
      if (mongoItemIDProperty != null && mongoItemIDProperty.length() > 0) {
        try {
          mongoItemID = mongoItemIDProperty;
        } catch (Exception e) {
          log.error("Property [MONGO_ITEM_FIELD] on properties file has an invalid value("
          + mongoItemIDProperty + ")");
          System.exit(0);
        }
      }
      log.info("MONGO_ITEM_FIELD read from configuration file: " + mongoItemID);
      
      String mongoPreferenceProperty = properties.getProperty("MONGO_PREFERENCE_FIELD");
      if (mongoPreferenceProperty != null && mongoPreferenceProperty.length() > 0) {
        try {
          mongoPreference = mongoPreferenceProperty;
        } catch (Exception e) {
          log.error("Property [MONGO_PREFERENCE_FIELD] on properties file has an invalid value("
          + mongoPreferenceProperty + ")");
          System.exit(0);
        }
      }
      log.info("MONGO_PREFERENCE_FIELD read from configuration file: " + mongoPreference);
      
      String similarityMeasureProperty = properties.getProperty("SIMILARITY_MEASURE");
      if (similarityMeasureProperty != null && similarityMeasureProperty.length() > 0) {
        try {
          similarityMeasure = similarityMeasureProperty;
        } catch (Exception e) {
          log.error("Property [SIMILARITY_MEASURE] on properties file has an invalid value("
          + similarityMeasureProperty + ")");
          System.exit(0);
        }
      }
      log.info("SIMILARITY_MEASURE read from configuration file: " + similarityMeasure);
      
      String neighborHoodProperty = properties.getProperty("NEIGHBORHOOD");
      if (neighborHoodProperty != null && neighborHoodProperty.length() > 0) {
        try {
          neighborhoodType = neighborHoodProperty;
        } catch (Exception e) {
          log.error("Property [NEIGHBORHOOD] on properties file has an invalid value("
          + neighborHoodProperty + ")");
          System.exit(0);
        }
      }
      log.info("NEIGHBORHOOD read from configuration file: " + neighborhoodType);
      
      
      String userThresholdProperty = properties.getProperty("USER_TH");
      if (userThresholdProperty != null && userThresholdProperty.length() > 0) {
        try {
          userThreshold = Double.parseDouble(userThresholdProperty);
        } catch (Exception e) {
          log.error("Property [USER_TH] on properties file has an invalid value("
          + userThresholdProperty + ")");
          System.exit(0);
        }
      }
      log.info("USER_TH read from configuration file: " + userThreshold);
      
      String neighborsNumberProperty = properties.getProperty("NEIGHBORS_NUMBER");
      if (neighborsNumberProperty != null && neighborsNumberProperty.length() > 0) {
        try {
          neighborsNumber = Integer.parseInt(neighborsNumberProperty);
        } catch (Exception e) {
          log.error("Property [NEIGHBORS_NUMBER] on properties file has an invalid value("
          + neighborsNumberProperty + ")");
          System.exit(0);
        }
      }
      log.info("NEIGHBORS_NUMBER read from configuration file: " + neighborsNumber);
      
      String maxRecommendationProperty = properties.getProperty("MAX_RECOMMENDATIONS");
      if (maxRecommendationProperty != null && maxRecommendationProperty.length() > 0) {
        try {
          maxRecommendations = Integer.parseInt(maxRecommendationProperty);
        } catch (Exception e) {
          log.error("Property [MAX_RECOMMENDATIONS] on properties file has an invalid value("
          + maxRecommendationProperty + ")");
          System.exit(0);
        }
      }
      log.info("MAX_RECOMMENDATIONS read from configuration file: " + maxRecommendations);
      
    } catch (Exception e) {
      log.error("Error while starting recommender.", e);
    }
  }
  
  /************************************************
   ******************** UTILS *******************
   ************************************************/
  private static void insertTestData() {
    try {
      userIsObject = true;
      itemIsObject = true;
      preferenceIsString = true;
      Mongo mongoDDBB = new Mongo(mongoHost , mongoPort);
      DB db = mongoDDBB.getDB(mongoDB);
      DBCollection collection = db.getCollection(mongoCollection);
      System.out.println("Creating test data in collection: " + mongoCollection);
      addMongoUserItem(collection, "4d6cd21685af61387c970001", "4d6cd21685af61387c971001", "0.5", true);
      addMongoUserItem(collection, "4d6cd21685af61387c970001", "4d6cd21685af61387c971002", "0.5", true);
      addMongoUserItem(collection, "4d6cd21685af61387c970001", "4d6cd21685af61387c971003", "0.5", true);
      addMongoUserItem(collection, "4d6cd21685af61387c970002", "4d6cd21685af61387c971002", "0.5", true);
      addMongoUserItem(collection, "4d6cd21685af61387c970002", "4d6cd21685af61387c971003", "0.5", true);
      addMongoUserItem(collection, "4d6cd21685af61387c970002", "4d6cd21685af61387c971004", "0.5", true);
      addMongoUserItem(collection, "4d6cd21685af61387c970003", "4d6cd21685af61387c971002", "0.5", true);
      addMongoUserItem(collection, "4d6cd21685af61387c970003", "4d6cd21685af61387c971003", "0.5", true);
      addMongoUserItem(collection, "4d6cd21685af61387c970004", "4d6cd21685af61387c971004", "0.5", true);
    } catch(Exception e) {
      System.out.println("[ERROR] Creating test data in collection: " + mongoCollection);
    }
  }
  
  private static void clean() {
    try {
      Mongo mongoDDBB = new Mongo(mongoHost , mongoPort);
      DB db = mongoDDBB.getDB(mongoDB);
      DBCollection collection = db.getCollection(mongoCollection);
      collection.remove(new BasicDBObject());
    } catch(Exception e) {
      System.out.println("[ERROR] Creating test data in collection: " + mongoCollection);
    }
  }
  
  private static void addMongoUserItem(DBCollection collection, String userID, String itemID, String preferenceValue, boolean p) {
    System.out.print("Creating new user..");
    BasicDBObject user = new BasicDBObject();
    Object userId = (userIsObject ? new ObjectId(userID) : userID);
    Object itemId = (itemIsObject ? new ObjectId(itemID) : itemID);
    user.put(mongoUserID, userId);
    user.put(mongoItemID, itemId);
    if (p) user.put(mongoPreference, (preferenceIsString ? preferenceValue : Float.parseFloat(preferenceValue)));
    user.put("created_at", new Date());
    System.out.println("OK. User: " + user.toString());
    collection.insert(user);
  }

}
