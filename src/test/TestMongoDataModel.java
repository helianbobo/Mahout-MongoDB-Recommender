package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.Before;

import java.text.SimpleDateFormat;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.paradigma.recommender.db.MongoDataModel;


/**
 * 
 * @author Fernando Tapia Rico
 *
 */
public class TestMongoDataModel{
  
  protected boolean userIsObject = true;
  protected boolean itemIsObject = true;
  protected boolean preferenceIsString = true;
  protected String mongoUserID = "user_id";
  protected String mongoItemID = "item_id";
  protected String mongoPreference = "preference";
  protected SimpleDateFormat defaultDateFormat = new SimpleDateFormat("EE MMM dd yyyy HH:mm:ss 'GMT'Z (zzz)");
  protected SimpleDateFormat dateFormat = defaultDateFormat;
  // User ObjectId and item long, no preference, default timestamp 
  protected DBCollection collectionOIi;
  //User long and item ObjectID, no preference, default timestamp 
  protected DBCollection collectioniOI;
  //User and item long, no preference, default timestamp 
  protected DBCollection collectionii;
  //User and item ObjectID, no preference, default timestamp 
  protected DBCollection collectionOIOI;
  //User and item ObjectID (string), no preference, default timestamp 
  protected DBCollection collectionOIsOIs;
  //User and item ObjectID, preference double, default timestamp 
  protected DBCollection collectionOIOIP;
  //User and item ObjectID, preference String, default timestamp 
  protected DBCollection collectionOIOIPS;
  
  protected DBCollection collectionMongoMap;

  /************************************************
   ******************** SET UP ********************
   ************************************************/
  
  @Before public void setUp() {
    try {
      Mongo mongoDDBB = new Mongo("localhost" , 27017);
      DB db = mongoDDBB.getDB("test_recommender");
      collectionOIi = db.getCollection("collectionOIi");
      collectioniOI = db.getCollection("collectioniOI");
      collectionii = db.getCollection("collectionii");
      collectionOIOI = db.getCollection("collectionOIOI");
      collectionOIsOIs = db.getCollection("collectionOIsOIs");
      collectionOIOIP = db.getCollection("collectionOIOIP");
      collectionOIOIPS = db.getCollection("collectionOIOIPS");
      collectionMongoMap = db.getCollection("mongo_data_model_map");
      System.out.println("Cleaning");
      clean();
      System.out.println("Creating collections");
      // Set collectionOIi
      userIsObject = true;
      itemIsObject = false;
      System.out.println("Creating collections: collectionOIi " + userIsObject + "    " + itemIsObject);
      addMongoUserItem(collectionOIi, "4d6cd21685af61387c970001", "1", null, false);
      addMongoUserItem(collectionOIi, "4d6cd21685af61387c970001", "2", null, false);
      addMongoUserItem(collectionOIi, "4d6cd21685af61387c970001", "3", null, false);
      addMongoUserItem(collectionOIi, "4d6cd21685af61387c970002", "2", null, false);
      addMongoUserItem(collectionOIi, "4d6cd21685af61387c970002", "3", null, false);
      addMongoUserItem(collectionOIi, "4d6cd21685af61387c970002", "4", null, false);
      addMongoUserItem(collectionOIi, "4d6cd21685af61387c970003", "2", null, false);
      addMongoUserItem(collectionOIi, "4d6cd21685af61387c970003", "3", null, false);
      addMongoUserItem(collectionOIi, "4d6cd21685af61387c970004", "4", null, false);
      // Set collectioniOI
      userIsObject = false;
      itemIsObject = true;
      System.out.println("Creating collections: collectioniOI " + userIsObject + "    " + itemIsObject);
      addMongoUserItem(collectioniOI, "1", "4d6cd21685af61387c971001", null, false);
      addMongoUserItem(collectioniOI, "1", "4d6cd21685af61387c971002", null, false);
      addMongoUserItem(collectioniOI, "1", "4d6cd21685af61387c971003", null, false);
      addMongoUserItem(collectioniOI, "2", "4d6cd21685af61387c971002", null, false);
      addMongoUserItem(collectioniOI, "2", "4d6cd21685af61387c971003", null, false);
      addMongoUserItem(collectioniOI, "2", "4d6cd21685af61387c971004", null, false);
      addMongoUserItem(collectioniOI, "3", "4d6cd21685af61387c971002", null, false);
      addMongoUserItem(collectioniOI, "3", "4d6cd21685af61387c971003", null, false);
      addMongoUserItem(collectioniOI, "4", "4d6cd21685af61387c971004", null, false);
      // Set collectionii
      userIsObject = false;
      itemIsObject = false;
      System.out.println("Creating collections: collectionii " + userIsObject + "    " + itemIsObject);
      addMongoUserItem(collectionii, "1", "1", null, false);
      addMongoUserItem(collectionii, "1", "2", null, false);
      addMongoUserItem(collectionii, "1", "3", null, false);
      addMongoUserItem(collectionii, "2", "2", null, false);
      addMongoUserItem(collectionii, "2", "3", null, false);
      addMongoUserItem(collectionii, "2", "4", null, false);
      addMongoUserItem(collectionii, "3", "2", null, false);
      addMongoUserItem(collectionii, "3", "3", null, false);
      addMongoUserItem(collectionii, "4", "4", null, false);
      // Set collectionOIOI
      userIsObject = true;
      itemIsObject = true;
      System.out.println("Creating collections: collectionOIOI " + userIsObject + "    " + itemIsObject);
      addMongoUserItem(collectionOIOI, "4d6cd21685af61387c970001", "4d6cd21685af61387c971001", null, false);
      addMongoUserItem(collectionOIOI, "4d6cd21685af61387c970001", "4d6cd21685af61387c971002", null, false);
      addMongoUserItem(collectionOIOI, "4d6cd21685af61387c970001", "4d6cd21685af61387c971003", null, false);
      addMongoUserItem(collectionOIOI, "4d6cd21685af61387c970002", "4d6cd21685af61387c971002", null, false);
      addMongoUserItem(collectionOIOI, "4d6cd21685af61387c970002", "4d6cd21685af61387c971003", null, false);
      addMongoUserItem(collectionOIOI, "4d6cd21685af61387c970002", "4d6cd21685af61387c971004", null, false);
      addMongoUserItem(collectionOIOI, "4d6cd21685af61387c970003", "4d6cd21685af61387c971002", null, false);
      addMongoUserItem(collectionOIOI, "4d6cd21685af61387c970003", "4d6cd21685af61387c971003", null, false);
      addMongoUserItem(collectionOIOI, "4d6cd21685af61387c970004", "4d6cd21685af61387c971004", null, false);
      // Set collectionOIsOIs
      userIsObject = false;
      itemIsObject = false;
      System.out.println("Creating collections: collectionOIsOIs " + userIsObject + "    " + itemIsObject);
      addMongoUserItem(collectionOIsOIs, "4d6cd21685af61387c970001", "4d6cd21685af61387c971001", null, false);
      addMongoUserItem(collectionOIsOIs, "4d6cd21685af61387c970001", "4d6cd21685af61387c971002", null, false);
      addMongoUserItem(collectionOIsOIs, "4d6cd21685af61387c970001", "4d6cd21685af61387c971003", null, false);
      addMongoUserItem(collectionOIsOIs, "4d6cd21685af61387c970002", "4d6cd21685af61387c971002", null, false);
      addMongoUserItem(collectionOIsOIs, "4d6cd21685af61387c970002", "4d6cd21685af61387c971003", null, false);
      addMongoUserItem(collectionOIsOIs, "4d6cd21685af61387c970002", "4d6cd21685af61387c971004", null, false);
      addMongoUserItem(collectionOIsOIs, "4d6cd21685af61387c970003", "4d6cd21685af61387c971002", null, false);
      addMongoUserItem(collectionOIsOIs, "4d6cd21685af61387c970003", "4d6cd21685af61387c971003", null, false);
      addMongoUserItem(collectionOIsOIs, "4d6cd21685af61387c970004", "4d6cd21685af61387c971004", null, false);
      // Set collectionOIOIP
      userIsObject = true;
      itemIsObject = true;
      preferenceIsString = false;
      System.out.println("Creating collections: collectionOIOIP " + userIsObject + "    " + itemIsObject);
      addMongoUserItem(collectionOIOIP, "4d6cd21685af61387c970001", "4d6cd21685af61387c971001", "0.5", true);
      addMongoUserItem(collectionOIOIP, "4d6cd21685af61387c970001", "4d6cd21685af61387c971002", "0.5", true);
      addMongoUserItem(collectionOIOIP, "4d6cd21685af61387c970001", "4d6cd21685af61387c971003", "0.5", true);
      addMongoUserItem(collectionOIOIP, "4d6cd21685af61387c970002", "4d6cd21685af61387c971002", "0.5", true);
      addMongoUserItem(collectionOIOIP, "4d6cd21685af61387c970002", "4d6cd21685af61387c971003", "0.5", true);
      addMongoUserItem(collectionOIOIP, "4d6cd21685af61387c970002", "4d6cd21685af61387c971004", "0.5", true);
      addMongoUserItem(collectionOIOIP, "4d6cd21685af61387c970003", "4d6cd21685af61387c971002", "0.5", true);
      addMongoUserItem(collectionOIOIP, "4d6cd21685af61387c970003", "4d6cd21685af61387c971003", "0.5", true);
      addMongoUserItem(collectionOIOIP, "4d6cd21685af61387c970004", "4d6cd21685af61387c971004", "0.5", true);
      // Set collectionOIOIPS
      userIsObject = true;
      itemIsObject = true;
      preferenceIsString = true;
      System.out.println("Creating collections: collectionOIOIPS " + userIsObject + "    " + itemIsObject);
      addMongoUserItem(collectionOIOIPS, "4d6cd21685af61387c970001", "4d6cd21685af61387c971001", "0.5", true);
      addMongoUserItem(collectionOIOIPS, "4d6cd21685af61387c970001", "4d6cd21685af61387c971002", "0.5", true);
      addMongoUserItem(collectionOIOIPS, "4d6cd21685af61387c970001", "4d6cd21685af61387c971003", "0.5", true);
      addMongoUserItem(collectionOIOIPS, "4d6cd21685af61387c970002", "4d6cd21685af61387c971002", "0.5", true);
      addMongoUserItem(collectionOIOIPS, "4d6cd21685af61387c970002", "4d6cd21685af61387c971003", "0.5", true);
      addMongoUserItem(collectionOIOIPS, "4d6cd21685af61387c970002", "4d6cd21685af61387c971004", "0.5", true);
      addMongoUserItem(collectionOIOIPS, "4d6cd21685af61387c970003", "4d6cd21685af61387c971002", "0.5", true);
      addMongoUserItem(collectionOIOIPS, "4d6cd21685af61387c970003", "4d6cd21685af61387c971003", "0.5", true);
      addMongoUserItem(collectionOIOIPS, "4d6cd21685af61387c970004", "4d6cd21685af61387c971004", "0.5", true);
    } catch(Exception e) {
      System.out.println("Error on setUp: " + e);
    }
    
  }
  
  /************************************************
   ****************** CONSTRUCTOR *****************
   ************************************************/
  
  @Test public void TestConstructors() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = true;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi", "collectioniOI", "collectionii", "collectionOIOI",
                            "collectionOIsOIs", "collectionOIOIP", "collectionOIOIPS"};
    for (String collection : collections) {
      try {
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collection, mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        assertEquals (model.getNumUsers(), 4);
        assertEquals (model.getNumItems(), 4);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collection + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  /************************************************
   ***************** REMOVE USERS *****************
   ************************************************/
  
  @Test public void TestRemoveExistingUserItemWithoutManage() {
    boolean mongoManage = false;
    boolean mongoFinalRemove = false;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"4d6cd21685af61387c970001",
                      "2",
                      "3",
                      "4d6cd21685af61387c970004",
                      "4d6cd21685af61387c970002",
                      "4d6cd21685af61387c970004",
                      "4d6cd21685af61387c970001"};
    int[][] numUserItems = {{4, 3},
                            {4, 4},
                            {4, 4},
                            {3, 4},
                            {4, 4},
                            {3, 4},
                            {4, 3}};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"3", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971002", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971001", "0.5"}};
    String[][] results = {{"2", "3"},
                          {"4d6cd21685af61387c971002", "4d6cd21685af61387c971003"},
                          {"2"},
                          {},
                          {"4d6cd21685af61387c971003", "4d6cd21685af61387c971004"},
                          {},
                          {"4d6cd21685af61387c971002", "4d6cd21685af61387c971003"}};
    DBCollection[] collectionObjects = {collectionOIi, 
                                  collectioniOI,
                                  collectionii,
                                  collectionOIOI, 
                                  collectionOIsOIs,
                                  collectionOIOIP,
                                  collectionOIOIPS}; 
    boolean[][] flags = {{true, false},
                         {false, true},
                         {false, false},
                         {true, true},
                         {false, false},
                         {true, true},
                         {true, true}}; 
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
        ArrayList<String> item = new ArrayList<String>();
        String user = users[collIndex];
        item.add(itemsList[collIndex][0]);
        item.add(itemsList[collIndex][1]);
        items.add(item);
        model.refreshData(user, items, false);
        assertEquals (model.getNumUsers(), numUserItems[collIndex][0]);
        assertEquals (model.getNumItems(), numUserItems[collIndex][1]);
        long userID = Long.parseLong(model.fromIdToLong(user, true));
        if (results[collIndex].length > 0) {
          FastIDSet userItems = model.getItemIDsFromUser(userID);
          assertEquals(userItems.toArray().length, results[collIndex].length);
          boolean contains = false;
          for (int index = 0; index < results[collIndex].length; index++) {
            contains = userItems.contains(Long.parseLong(model.fromIdToLong(results[collIndex][index], false)));
            if (!contains) break;
          }
          assertTrue(contains);
        }
        userIsObject = flags[collIndex][0];
        itemIsObject = flags[collIndex][1];
        assertTrue(isUserItemInDB(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
        assertFalse(hasDeletedAtField(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestRemoveExistingUserItemWithManage() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = false;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"4d6cd21685af61387c970001",
                      "2",
                      "3",
                      "4d6cd21685af61387c970004",
                      "4d6cd21685af61387c970002",
                      "4d6cd21685af61387c970004",
                      "4d6cd21685af61387c970001"};
    int[][] numUserItems = {{4, 3},
                            {4, 4},
                            {4, 4},
                            {3, 4},
                            {4, 4},
                            {3, 4},
                            {4, 3}};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"3", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971002", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971001", "0.5"}};
    String[][] results = {{"2", "3"},
                          {"4d6cd21685af61387c971002", "4d6cd21685af61387c971003"},
                          {"2"},
                          {},
                          {"4d6cd21685af61387c971003", "4d6cd21685af61387c971004"},
                          {},
                          {"4d6cd21685af61387c971002", "4d6cd21685af61387c971003"}};
    DBCollection[] collectionObjects = {collectionOIi, 
                                  collectioniOI,
                                  collectionii,
                                  collectionOIOI, 
                                  collectionOIsOIs,
                                  collectionOIOIP,
                                  collectionOIOIPS}; 
    boolean[][] flags = {{true, false},
                         {false, true},
                         {false, false},
                         {true, true},
                         {false, false},
                         {true, true},
                         {true, true}}; 
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
        ArrayList<String> item = new ArrayList<String>();
        String user = users[collIndex];
        item.add(itemsList[collIndex][0]);
        item.add(itemsList[collIndex][1]);
        items.add(item);
        model.refreshData(user, items, false);
        assertEquals (model.getNumUsers(), numUserItems[collIndex][0]);
        assertEquals (model.getNumItems(), numUserItems[collIndex][1]);
        long userID = Long.parseLong(model.fromIdToLong(user, true));
        if (results[collIndex].length > 0) {
          FastIDSet userItems = model.getItemIDsFromUser(userID);
          assertEquals(userItems.toArray().length, results[collIndex].length);
          boolean contains = false;
          for (int index = 0; index < results[collIndex].length; index++) {
            contains = userItems.contains(Long.parseLong(model.fromIdToLong(results[collIndex][index], false)));
            if (!contains) break;
          }
          assertTrue(contains);
        }
        userIsObject = flags[collIndex][0];
        itemIsObject = flags[collIndex][1];
        assertTrue(isUserItemInDB(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
        assertTrue(hasDeletedAtField(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestRemoveExistingUserItemWithManageFinal() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = true;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"4d6cd21685af61387c970001",
                      "2",
                      "3",
                      "4d6cd21685af61387c970004",
                      "4d6cd21685af61387c970002",
                      "4d6cd21685af61387c970004",
                      "4d6cd21685af61387c970001"};
    int[][] numUserItems = {{4, 3},
                            {4, 4},
                            {4, 4},
                            {3, 4},
                            {4, 4},
                            {3, 4},
                            {4, 3}};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"3", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971002", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971001", "0.5"}};
    String[][] results = {{"2", "3"},
                          {"4d6cd21685af61387c971002", "4d6cd21685af61387c971003"},
                          {"2"},
                          {},
                          {"4d6cd21685af61387c971003", "4d6cd21685af61387c971004"},
                          {},
                          {"4d6cd21685af61387c971002", "4d6cd21685af61387c971003"}};
    DBCollection[] collectionObjects = {collectionOIi, 
                                  collectioniOI,
                                  collectionii,
                                  collectionOIOI, 
                                  collectionOIsOIs,
                                  collectionOIOIP,
                                  collectionOIOIPS}; 
    boolean[][] flags = {{true, false},
                         {false, true},
                         {false, false},
                         {true, true},
                         {false, false},
                         {true, true},
                         {true, true}}; 
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
        ArrayList<String> item = new ArrayList<String>();
        String user = users[collIndex];
        item.add(itemsList[collIndex][0]);
        item.add(itemsList[collIndex][1]);
        items.add(item);
        model.refreshData(user, items, false);
        assertEquals (model.getNumUsers(), numUserItems[collIndex][0]);
        assertEquals (model.getNumItems(), numUserItems[collIndex][1]);
        long userID = Long.parseLong(model.fromIdToLong(user, true));
        if (results[collIndex].length > 0) {
          FastIDSet userItems = model.getItemIDsFromUser(userID);
          assertEquals(userItems.toArray().length, results[collIndex].length);
          boolean contains = false;
          for (int index = 0; index < results[collIndex].length; index++) {
            contains = userItems.contains(Long.parseLong(model.fromIdToLong(results[collIndex][index], false)));
            if (!contains) break;
          }
          assertTrue(contains);
        }
        userIsObject = flags[collIndex][0];
        itemIsObject = flags[collIndex][1];
        assertFalse(isUserItemInDB(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestRemoveUnexistingUser() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = true;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"4d6cd21685af61387c970007",
                      "7",
                      "7",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970007"};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"3", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971002", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971001", "0.5"}};
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
        ArrayList<String> item = new ArrayList<String>();
        String user = users[collIndex];
        item.add(itemsList[collIndex][0]);
        item.add(itemsList[collIndex][1]);
        items.add(item);
        model.refreshData(user, items, false);
        fail("An NoSuchUserException or NoSuchItemException had to be thrown. Collection: " + collections[collIndex]);
      } catch (NoSuchUserException e) {
        assertTrue(true);
      } catch (NoSuchItemException e) {
        assertTrue(true);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestRemoveUserOrItemEmpty() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = true;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"",
                      "7",
                      "",
                      "",
                      "4d6cd21685af61387c970007",
                      "",
                      ""};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"", "0.5"},
                            {"3", "0.5"},
                            {"", "0.5"},
                            {"", "0.5"},
                            {"", "0.5"},
                            {"4d6cd21685af61387c971001", "0.5"}};
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
        ArrayList<String> item = new ArrayList<String>();
        String user = users[collIndex];
        item.add(itemsList[collIndex][0]);
        item.add(itemsList[collIndex][1]);
        items.add(item);
        model.refreshData(user, items, false);
        fail("An IllegalArgumentException had to be thrown. Collection: " + collections[collIndex]);
      } catch (IllegalArgumentException e) {
        assertTrue(true);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestRemoveUserOrItemNull() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = true;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {null,
                      "7",
                      null,
                      null,
                      null,
                      null,
                      null};
    String[][] itemsList = {{null, "0.5"}, 
                            {null, "0.5"},
                            {"3", "0.5"},
                            {null, "0.5"},
                            {null, "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            null};
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
        ArrayList<String> item = new ArrayList<String>();
        String user = users[collIndex];
        item.add(itemsList[collIndex][0]);
        item.add(itemsList[collIndex][1]);
        items.add(item);
        model.refreshData(user, items, false);
        fail("A NullPointerException had to be thrown. Collection: " + collections[collIndex]);
      } catch (NullPointerException e) {
        assertTrue(true);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestRemoveUserOrItemNotValidID() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = true;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"1234",
                      "a7",
                      "b7",
                      "1234",
                      "1234",
                      "1234",
                      "1234"};
    String[][] itemsList = {{"1b", "0.5"}, 
                            {"2c1b2", "0.5"},
                            {"2c1b2", "0.5"},
                            {"2c1b2", "0.5"},
                            {"2c1b2", "0.5"},
                            {"2c1b2", "0.5"},
                            {"2c1b2", "0.5"}};
    boolean[][] flags = {{true, false},
                         {false, true},
                         {false, false},
                         {true, true},
                         {false, false},
                         {true, true},
                         {true, true}}; 
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
        ArrayList<String> item = new ArrayList<String>();
        String user = users[collIndex];
        item.add(itemsList[collIndex][0]);
        item.add(itemsList[collIndex][1]);
        items.add(item);
        model.refreshData(user, items, false);
        if (flags[collIndex][0] || flags[collIndex][1]) {
          fail("An IllegalArgumentException had to be thrown. Collection: " + collections[collIndex]);
        }
      } catch (IllegalArgumentException e) {
        if (flags[collIndex][0] || flags[collIndex][1]) {
          assertTrue(true);
        } else {
          fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
        }
      } catch (NoSuchUserException e) {
        if (flags[collIndex][0] || flags[collIndex][1]) {
          fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e);
        }
      } catch (NoSuchItemException e) {
        if (flags[collIndex][0] || flags[collIndex][1]) {
          fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e);
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  /************************************************
   ******************* ADD USERS ******************
   ************************************************/
  
  @Test public void TestAddUserItemWithoutManage() {
    boolean mongoManage = false;
    boolean mongoFinalRemove = false;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"4d6cd21685af61387c970001",
                      "7",
                      "3",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970002",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970001"};
    int[][] numUserItems = {{4, 4},
                            {5, 5},
                            {4, 4},
                            {5, 5},
                            {4, 5},
                            {5, 4},
                            {4, 5}};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"4d6cd21685af61387c971007", "0.5"},
                            {"3", "0.5"},
                            {"4d6cd21685af61387c971006", "0.5"},
                            {"4d6cd21685af61387c971008", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971009", "0.5"}};
    String[][] results = {{"1", "2", "3"},
                          {"4d6cd21685af61387c971007"},
                          {"2", "3"},
                          {"4d6cd21685af61387c971006"},
                          {"4d6cd21685af61387c971002", "4d6cd21685af61387c971003", "4d6cd21685af61387c971004", "4d6cd21685af61387c971008"},
                          {"4d6cd21685af61387c971004"},
                          {"4d6cd21685af61387c971001", "4d6cd21685af61387c971002", "4d6cd21685af61387c971003", "4d6cd21685af61387c971009"}};
    DBCollection[] collectionObjects = {collectionOIi, 
                                  collectioniOI,
                                  collectionii,
                                  collectionOIOI, 
                                  collectionOIsOIs,
                                  collectionOIOIP,
                                  collectionOIOIPS}; 
    boolean[][] flags = {{true, false},
                         {false, true},
                         {false, false},
                         {true, true},
                         {false, false},
                         {true, true},
                         {true, true}}; 
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
        ArrayList<String> item = new ArrayList<String>();
        String user = users[collIndex];
        item.add(itemsList[collIndex][0]);
        item.add(itemsList[collIndex][1]);
        items.add(item);
        model.refreshData(user, items, true);
        assertEquals (model.getNumUsers(), numUserItems[collIndex][0]);
        assertEquals (model.getNumItems(), numUserItems[collIndex][1]);
        long userID = Long.parseLong(model.fromIdToLong(user, true));
        FastIDSet userItems = model.getItemIDsFromUser(userID);
        assertEquals(userItems.toArray().length, results[collIndex].length);
        boolean contains = false;
        for (int index = 0; index < results[collIndex].length; index++) {
          contains = userItems.contains(Long.parseLong(model.fromIdToLong(results[collIndex][index], false)));
          if (!contains) break;
        }
        assertTrue(contains);
        userIsObject = flags[collIndex][0];
        itemIsObject = flags[collIndex][1];
        if (numUserItems[collIndex][0] == 5) {
          assertFalse(isUserItemInDB(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
        }
        assertFalse(hasDeletedAtField(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestAddUserItemWithManage() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = false;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"4d6cd21685af61387c970001",
                      "7",
                      "3",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970002",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970001"};
    int[][] numUserItems = {{4, 4},
                            {5, 5},
                            {4, 4},
                            {5, 5},
                            {4, 5},
                            {5, 4},
                            {4, 5}};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"4d6cd21685af61387c971007", "0.5"},
                            {"3", "0.5"},
                            {"4d6cd21685af61387c971006", "0.5"},
                            {"4d6cd21685af61387c971008", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971009", "0.5"}};
    String[][] results = {{"1", "2", "3"},
                          {"4d6cd21685af61387c971007"},
                          {"2", "3"},
                          {"4d6cd21685af61387c971006"},
                          {"4d6cd21685af61387c971002", "4d6cd21685af61387c971003", "4d6cd21685af61387c971004", "4d6cd21685af61387c971008"},
                          {"4d6cd21685af61387c971004"},
                          {"4d6cd21685af61387c971001", "4d6cd21685af61387c971002", "4d6cd21685af61387c971003", "4d6cd21685af61387c971009"}};
    DBCollection[] collectionObjects = {collectionOIi, 
                                  collectioniOI,
                                  collectionii,
                                  collectionOIOI, 
                                  collectionOIsOIs,
                                  collectionOIOIP,
                                  collectionOIOIPS}; 
    boolean[][] flags = {{true, false},
                         {false, true},
                         {false, false},
                         {true, true},
                         {false, false},
                         {true, true},
                         {true, true}}; 
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
        ArrayList<String> item = new ArrayList<String>();
        String user = users[collIndex];
        item.add(itemsList[collIndex][0]);
        item.add(itemsList[collIndex][1]);
        items.add(item);
        model.refreshData(user, items, true);
        assertEquals (model.getNumUsers(), numUserItems[collIndex][0]);
        assertEquals (model.getNumItems(), numUserItems[collIndex][1]);
        long userID = Long.parseLong(model.fromIdToLong(user, true));
        FastIDSet userItems = model.getItemIDsFromUser(userID);
        assertEquals(userItems.toArray().length, results[collIndex].length);
        boolean contains = false;
        for (int index = 0; index < results[collIndex].length; index++) {
          contains = userItems.contains(Long.parseLong(model.fromIdToLong(results[collIndex][index], false)));
          if (!contains) break;
        }
        assertTrue(contains);
        userIsObject = flags[collIndex][0];
        itemIsObject = flags[collIndex][1];
        assertTrue(isUserItemInDB(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
        assertFalse(hasDeletedAtField(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestAddUserOrItemEmpty() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = false;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"",
                      "7",
                      "3",
                      "",
                      "4d6cd21685af61387c970002",
                      "",
                      "4d6cd21685af61387c970001"};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"", "0.5"},
                            {"", "0.5"},
                            {"4d6cd21685af61387c971006", "0.5"},
                            {"", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"", "0.5"}};
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
        ArrayList<String> item = new ArrayList<String>();
        String user = users[collIndex];
        item.add(itemsList[collIndex][0]);
        item.add(itemsList[collIndex][1]);
        items.add(item);
        model.refreshData(user, items, true);
        fail("An IllegalArgumentException had to be thrown. Collection: " + collections[collIndex]);
      } catch (IllegalArgumentException e) {
         assertTrue(true);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestAddUserOrItemNull() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = false;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {null,
                      "7",
                      "3",
                      null,
                      null,
                      "4d6cd21685af61387c970007",
                      null};
    String[][] itemsList = {{"1", "0.5"}, 
                            {null, "0.5"},
                            null,
                            {"4d6cd21685af61387c971006", "0.5"},
                            {"4d6cd21685af61387c971008", "0.5"},
                            {null, "0.5"},
                            {"4d6cd21685af61387c971009", "0.5"}};
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
        ArrayList<String> item = new ArrayList<String>();
        String user = users[collIndex];
        item.add(itemsList[collIndex][0]);
        item.add(itemsList[collIndex][1]);
        items.add(item);
        model.refreshData(user, items, true);
        fail("A NullPointerException had to be thrown. Collection: " + collections[collIndex]);
      } catch (NullPointerException e) {
        assertTrue(true);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestAddUserOrItemNotValidID() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = false;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"1b34",
                      "b7",
                      "3",
                      "1b34",
                      "1b34",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970001"};
    int[][] numUserItems = {null,
                            null,
                            {4, 5},
                            null,
                            {5, 5},
                            null,
                            null};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"b4", "0.5"},
                            {"b3", "0.5"},
                            {"4d6cd21685af61387c971006", "0.5"},
                            {"4d6cd21685af61387c971008", "0.5"},
                            {"1b34", "0.5"},
                            {"1b34", "0.5"}};
    String[][] results = {null,
                          null,
                          {"2", "3", "b3"},
                          null,
                          {"4d6cd21685af61387c971008"},
                          null,
                          null};
    DBCollection[] collectionObjects = {null, 
                                  null,
                                  collectionii,
                                  null, 
                                  collectionOIsOIs,
                                  null,
                                  null}; 
    boolean[][] flags = {{true, false},
                         {false, true},
                         {false, false},
                         {true, true},
                         {false, false},
                         {true, true},
                         {true, true}}; 
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        ArrayList<ArrayList<String>> items = new ArrayList<ArrayList<String>>();
        ArrayList<String> item = new ArrayList<String>();
        String user = users[collIndex];
        item.add(itemsList[collIndex][0]);
        item.add(itemsList[collIndex][1]);
        items.add(item);
        model.refreshData(user, items, true);
        if (flags[collIndex][0] || flags[collIndex][1]) {
          fail("An IllegalArgumentException had to be thrown. Collection: " + collections[collIndex]);
        } else {
          assertEquals (model.getNumUsers(), numUserItems[collIndex][0]);
          assertEquals (model.getNumItems(), numUserItems[collIndex][1]);
          long userID = Long.parseLong(model.fromIdToLong(user, true));
          FastIDSet userItems = model.getItemIDsFromUser(userID);
          assertEquals(userItems.toArray().length, results[collIndex].length);
          boolean contains = false;
          for (int index = 0; index < results[collIndex].length; index++) {
            contains = userItems.contains(Long.parseLong(model.fromIdToLong(results[collIndex][index], false)));
            if (!contains) break;
          }
          assertTrue(contains);
          userIsObject = flags[collIndex][0];
          itemIsObject = flags[collIndex][1];
          assertTrue(isUserItemInDB(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
          assertFalse(hasDeletedAtField(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
        }
      } catch (IllegalArgumentException e ) {
        if (flags[collIndex][0] || flags[collIndex][1]) {
          assertTrue(true);
        } else {
          fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  /************************************************
   ******************** REFRESH *******************
   ************************************************/
  
  @Test public void TestRefreshRemove() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = true;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"4d6cd21685af61387c970001",
                      "2",
                      "3",
                      "4d6cd21685af61387c970004",
                      "4d6cd21685af61387c970002",
                      "4d6cd21685af61387c970004",
                      "4d6cd21685af61387c970001"};
    int[][] numUserItems = {{4, 3},
                            {4, 4},
                            {4, 4},
                            {3, 4},
                            {4, 4},
                            {3, 4},
                            {4, 3}};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"3", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971002", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971001", "0.5"}};
    String[][] results = {{"2", "3"},
                          {"4d6cd21685af61387c971002", "4d6cd21685af61387c971003"},
                          {"2"},
                          {},
                          {"4d6cd21685af61387c971003", "4d6cd21685af61387c971004"},
                          {},
                          {"4d6cd21685af61387c971002", "4d6cd21685af61387c971003"}};
    DBCollection[] collectionObjects = {collectionOIi, 
                                  collectioniOI,
                                  collectionii,
                                  collectionOIOI, 
                                  collectionOIsOIs,
                                  collectionOIOIP,
                                  collectionOIOIPS}; 
    boolean[][] flags = {{true, false},
                         {false, true},
                         {false, false},
                         {true, true},
                         {false, false},
                         {true, true},
                         {true, true}}; 
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        userIsObject = flags[collIndex][0];
        itemIsObject = flags[collIndex][1];
        addDeleteTag(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]);
        model.refresh(null);
        assertEquals (model.getNumUsers(), numUserItems[collIndex][0]);
        assertEquals (model.getNumItems(), numUserItems[collIndex][1]);
        long userID = Long.parseLong(model.fromIdToLong(users[collIndex], true));
        if (results[collIndex].length > 0) {
          FastIDSet userItems = model.getItemIDsFromUser(userID);
          assertEquals(userItems.toArray().length, results[collIndex].length);
          boolean contains = false;
          for (int index = 0; index < results[collIndex].length; index++) {
            contains = userItems.contains(Long.parseLong(model.fromIdToLong(results[collIndex][index], false)));
            if (!contains) break;
          }
          assertTrue(contains);
        }
        assertFalse(isUserItemInDB(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestRefreshAdd() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = false;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"4d6cd21685af61387c970001",
                      "7",
                      "3",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970002",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970001"};
    int[][] numUserItems = {{4, 4},
                            {5, 5},
                            {4, 4},
                            {5, 5},
                            {4, 5},
                            {5, 4},
                            {4, 5}};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"4d6cd21685af61387c971007", "0.5"},
                            {"3", "0.5"},
                            {"4d6cd21685af61387c971006", "0.5"},
                            {"4d6cd21685af61387c971008", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971009", "0.5"}};
    String[][] results = {{"1", "2", "3"},
                          {"4d6cd21685af61387c971007"},
                          {"2", "3"},
                          {"4d6cd21685af61387c971006"},
                          {"4d6cd21685af61387c971002", "4d6cd21685af61387c971003", "4d6cd21685af61387c971004", "4d6cd21685af61387c971008"},
                          {"4d6cd21685af61387c971004"},
                          {"4d6cd21685af61387c971001", "4d6cd21685af61387c971002", "4d6cd21685af61387c971003", "4d6cd21685af61387c971009"}};
    DBCollection[] collectionObjects = {collectionOIi, 
                                  collectioniOI,
                                  collectionii,
                                  collectionOIOI, 
                                  collectionOIsOIs,
                                  collectionOIOIP,
                                  collectionOIOIPS}; 
    boolean[][] flags = {{true, false, true},
                         {false, true, true},
                         {false, false, true},
                         {true, true, true},
                         {false, false, true},
                         {true, true, false},
                         {true, true, true}}; 
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        userIsObject = flags[collIndex][0];
        itemIsObject = flags[collIndex][1];
        addMongoUserItem(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0], itemsList[collIndex][1], flags[collIndex][2]);
        model.refresh(null);
        assertEquals (model.getNumUsers(), numUserItems[collIndex][0]);
        assertEquals (model.getNumItems(), numUserItems[collIndex][1]);
        long userID = Long.parseLong(model.fromIdToLong(users[collIndex], true));
        FastIDSet userItems = model.getItemIDsFromUser(userID);
        assertEquals(userItems.toArray().length, results[collIndex].length);
        boolean contains = false;
        for (int index = 0; index < results[collIndex].length; index++) {
          contains = userItems.contains(Long.parseLong(model.fromIdToLong(results[collIndex][index], false)));
          if (!contains) break;
        }
        assertTrue(contains);
        assertTrue(isUserItemInDB(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
        assertFalse(hasDeletedAtField(collectionObjects[collIndex], users[collIndex], itemsList[collIndex][0]));
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestRefreshEmptyUser() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = false;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"4d6cd21685af61387c971007", "0.5"},
                            {"3", "0.5"},
                            {"4d6cd21685af61387c971006", "0.5"},
                            {"4d6cd21685af61387c971008", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971009", "0.5"}};
    DBCollection[] collectionObjects = {collectionOIi,
                                        collectioniOI,
                                        collectionii,
                                        collectionOIOI,
                                        collectionOIsOIs,
                                        collectionOIOIP,
                                        collectionOIOIPS}; 
    boolean[][] flags = {{true, false, true},
                         {false, true, true},
                         {false, false, true},
                         {true, true, true},
                         {false, false, true},
                         {true, true, false},
                         {true, true, true}}; 
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        BasicDBObject user = new BasicDBObject();
        Object itemId = (flags[collIndex][1] ? new ObjectId(itemsList[collIndex][0]) : itemsList[collIndex][0]);
        user.put(mongoItemID, itemId);
        if (flags[collIndex][2]) user.put(mongoPreference, (preferenceIsString ? itemsList[collIndex][1] : Float.parseFloat(itemsList[collIndex][1])));
        user.put("created_at", new Date());
        System.out.println("OK. User: " + user.toString());
        collectionObjects[collIndex].insert(user);
        model.refresh(null);
        fail("A NullPointerException had to be thrown. Collection: " + collections[collIndex]);
      } catch (NullPointerException e) {
        assertTrue(true);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestRefreshEmptyItem() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = false;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"4d6cd21685af61387c970001",
                      "7",
                      "3",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970002",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970001"};
    DBCollection[] collectionObjects = {collectionOIi,
                                        collectioniOI,
                                        collectionii,
                                        collectionOIOI,
                                        collectionOIsOIs,
                                        collectionOIOIP,
                                        collectionOIOIPS}; 
    boolean[][] flags = {{true, false, true},
                         {false, true, true},
                         {false, false, true},
                         {true, true, true},
                         {false, false, true},
                         {true, true, false},
                         {true, true, true}}; 
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        BasicDBObject user = new BasicDBObject();
        Object userId = (flags[collIndex][0] ? new ObjectId(users[collIndex]) : users[collIndex]);
        user.put(mongoUserID, userId);
        user.put("created_at", new Date());
        System.out.println("OK. User: " + user.toString());
        collectionObjects[collIndex].insert(user);
        model.refresh(null);
        fail("A NullPointerException had to be thrown. Collection: " + collections[collIndex]);
      } catch (NullPointerException e) {
        assertTrue(true);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  @Test public void TestRefreshWihtoutCreatedAtTag() {
    boolean mongoManage = true;
    boolean mongoFinalRemove = false;
    SimpleDateFormat dateFormat = null;
    String[] collections = {"collectionOIi",
                            "collectioniOI",
                            "collectionii",
                            "collectionOIOI",
                            "collectionOIsOIs",
                            "collectionOIOIP",
                            "collectionOIOIPS"};
    String[] users = {"4d6cd21685af61387c970001",
                      "7",
                      "3",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970002",
                      "4d6cd21685af61387c970007",
                      "4d6cd21685af61387c970001"};
    String[][] itemsList = {{"1", "0.5"}, 
                            {"4d6cd21685af61387c971007", "0.5"},
                            {"3", "0.5"},
                            {"4d6cd21685af61387c971006", "0.5"},
                            {"4d6cd21685af61387c971008", "0.5"},
                            {"4d6cd21685af61387c971004", "0.5"},
                            {"4d6cd21685af61387c971009", "0.5"}};
    DBCollection[] collectionObjects = {collectionOIi,
                                        collectioniOI,
                                        collectionii,
                                        collectionOIOI,
                                        collectionOIsOIs,
                                        collectionOIOIP,
                                        collectionOIOIPS}; 
    boolean[][] flags = {{true, false, true},
                         {false, true, true},
                         {false, false, true},
                         {true, true, true},
                         {false, false, true},
                         {true, true, false},
                         {true, true, true}}; 
    for (int collIndex = 0; collIndex < collections.length; collIndex++) {
      try {
        System.out.println(collections[collIndex]);
        MongoDataModel model = new MongoDataModel("localhost", 27017, "test_recommender",
              collections[collIndex], mongoManage, mongoFinalRemove, dateFormat,
              mongoUserID, mongoItemID, mongoPreference);
        BasicDBObject user = new BasicDBObject();
        Object userId = (flags[collIndex][0] ? new ObjectId(users[collIndex]) : users[collIndex]);
        Object itemId = (flags[collIndex][1] ? new ObjectId(itemsList[collIndex][0]) : itemsList[collIndex][0]);
        user.put(mongoItemID, itemId);
        if (flags[collIndex][2]) user.put(mongoPreference, (preferenceIsString ? itemsList[collIndex][1] : Float.parseFloat(itemsList[collIndex][1])));
        user.put(mongoUserID, userId);
        System.out.println("OK. User: " + user.toString());
        collectionObjects[collIndex].insert(user);
        model.refresh(null);
        assertEquals (model.getNumUsers(), 4);
        assertEquals (model.getNumItems(), 4);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should not have been thrown. Collection: " + collections[collIndex] + ". Error: " + e); 
      }
      clean_mongo_map();
    }
  }
  
  /************************************************
   ******************** UTILS *******************
   ************************************************/
  private void addMongoUserItem(DBCollection collection, String userID, String itemID, String preferenceValue, boolean p) {
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
  
  private void addDeleteTag(DBCollection collection, String userID, String itemID) {
    BasicDBObject query = new BasicDBObject();
    query.put(mongoUserID, (userIsObject ? new ObjectId(userID) : userID));
    query.put(mongoItemID, (itemIsObject ? new ObjectId(itemID) : itemID));
    BasicDBObject update = new BasicDBObject();
    update.put("$set", new BasicDBObject("deleted_at", new Date()));
    System.out.println(collection.update(query, update).toString());
  }
  
  
  private void clean_mongo_map() {
    collectionMongoMap.remove(new BasicDBObject());
  }
  
  private void clean() {
    collectionOIi.remove(new BasicDBObject());
    collectioniOI.remove(new BasicDBObject());
    collectionii.remove(new BasicDBObject());
    collectionOIOI.remove(new BasicDBObject());
    collectionOIsOIs.remove(new BasicDBObject());
    collectionOIOIP.remove(new BasicDBObject());
    collectionOIOIPS.remove(new BasicDBObject());
  }
  
  private boolean isUserItemInDB(DBCollection collection, String userID, String itemID) {
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
  
  private boolean hasDeletedAtField(DBCollection collection, String userID, String itemID) {
    BasicDBObject query = new BasicDBObject();
    Object userId = (userIsObject ? new ObjectId(userID) : userID);
    Object itemId = (itemIsObject ? new ObjectId(itemID) : itemID);
    query.put(mongoUserID, userId);
    query.put(mongoItemID, itemId);
    DBObject row = collection.findOne(query);
    if (row != null) {
      HashMap<String, Object> rowContent = (HashMap<String, Object>) row.toMap();
      return rowContent.containsKey("deleted_at");
    } else {
      return false;
    }
  }
  
}
