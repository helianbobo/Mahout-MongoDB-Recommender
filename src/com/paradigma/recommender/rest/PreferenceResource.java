package com.paradigma.recommender.rest;

import java.util.ArrayList;
import java.util.HashMap;

import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import javax.servlet.*;

import org.bson.types.ObjectId;
import org.slf4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.paradigma.recommender.GeneralRecommender;


/**
 * 
 * @author Alvaro Martin Fraguas - Paradigma Tecnologico
 *
 */
@Path(value="/recommender_preferences")
public class PreferenceResource {

  
  static @Context ServletContext servletContext;

  
  // Delete a preference of a user
  @DELETE
  @Path(value="/{preferenceID}.xml")
  public void deletePreference(@PathParam(value="preferenceID") String preferenceID) {
    ArrayList<String> rawPreference = getPreference(preferenceID);
    refreshPreference(rawPreference, false, servletContext);
  }


  // Need to pass the servletContext as an argument to be able to call this method from other resource classes
  static void refreshPreference (ArrayList<String> rawPreference, boolean add, ServletContext servletContext) {
    // Build the preferences
    ArrayList<ArrayList<String>> preferences = new ArrayList<ArrayList<String>> ();
    ArrayList<String> preference = new ArrayList<String> ();
    preference.add(rawPreference.get(1)); // Add the itemID
    preference.add(rawPreference.get(2)); // Add the preference value    
    preferences.add(preference);

    // Refresh the recommender
    GeneralRecommender recommender = (GeneralRecommender) servletContext.getAttribute("recommender");
    try {
      recommender.refreshData(rawPreference.get(0), preferences, add);
    } catch (Exception e) {
    }
    return;
  }
  
  
  private static ArrayList<String> getPreference (String preferenceID) {
    // Get the context
    String mongoHost = (String) servletContext.getAttribute("mongoHost");
    int mongoPort = ((Integer) servletContext.getAttribute("mongoPort")).intValue();
    String mongoDB = (String) servletContext.getAttribute("mongoDB");
    String mongoCollection = (String) servletContext.getAttribute("mongoCollection");
    String user_id_field = (String) servletContext.getAttribute("mongoUserID");
    String item_id_field = (String) servletContext.getAttribute("mongoItemID");
    String preference_field = (String) servletContext.getAttribute("mongoPreference");
    boolean mongoAuth = ((Boolean) servletContext.getAttribute("mongoAuth")).booleanValue();
    
    // Get the values from the DB
    Mongo mongoDDBB = null;
    try {
      mongoDDBB = new Mongo(mongoHost , mongoPort);
    } catch (Exception e) {
      Logger log = (Logger) servletContext.getAttribute("log");
      log.error("Exception thrown while connecting to the MongoDB: " + e.getMessage());
    }

    DB db = mongoDDBB.getDB(mongoDB);
    
    if (mongoAuth) {
      String mongoUsername = (String) servletContext.getAttribute("mongoUsername");
      String mongoPassword = (String) servletContext.getAttribute("mongoPassword");
      db.authenticate(mongoUsername, mongoPassword.toCharArray());
    }
    
    DBCollection collection = db.getCollection(mongoCollection);
    BasicDBObject user = new BasicDBObject();
    user.put("_id", new ObjectId(preferenceID));
    DBObject userObject = collection.findOne(user);
    HashMap<String, String> userMap = (HashMap<String, String>) userObject.toMap();
    String user_id = userMap.get(user_id_field);
    String item_id = userMap.get(item_id_field);
    String preference_value = userMap.get(preference_field);
    
    // Build the preference
    ArrayList<String> preference = new ArrayList<String> ();
    preference.add(user_id);
    preference.add(item_id);
    preference.add(preference_value);

    return preference;
  }
  
}
