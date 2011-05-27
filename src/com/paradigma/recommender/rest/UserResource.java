package com.paradigma.recommender.rest;

import java.util.ArrayList;
import java.util.Iterator;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import javax.servlet.*;

import com.paradigma.recommender.GeneralRecommender;


/**
 * 
 * @author Alvaro Martin Fraguas - Paradigma Tecnologico
 *
 */
@Path(value="/users")
public class UserResource {

  static @Context ServletContext servletContext;
  static String XMLDeclaration = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
  
  
  // Get the recommended users of a user
  @GET
  @Path(value="/{userID}/recommendedUsers")
  @Produces(value="application/xml")
  public String getRecommendedUsers(@PathParam(value="userID") String userID) {
    GeneralRecommender recommender = (GeneralRecommender) servletContext.getAttribute("recommender");
    String response = XMLDeclaration;
    try {
      response = buildRecommendedResourceResponse("recommendedUser", recommender.recommend(userID, null, true));
    } catch (Exception e) {}
    return response;
  }

  
  // Get the recommended items of a user
  @GET
  @Path(value="/{userID}/recommendedItems")
  @Produces(value="application/xml")
  public String getRecommendedItems(@PathParam(value="userID") String userID) {
    GeneralRecommender recommender = (GeneralRecommender) servletContext.getAttribute("recommender");
    String response = XMLDeclaration;
    try {
      response = buildRecommendedResourceResponse("recommendedItem", recommender.recommend(userID, null, false));
    } catch (Exception e) {}
    return response;
  }

  
  /*
   * ====================================================================================
   *
   *
   *                               PRIVATE METHODS
   *
   *
   * ====================================================================================
   */
  
  
  private static String buildRecommendedResourceResponse (String resourceName, ArrayList<String> recommendations) {
    Iterator<String> it = recommendations.iterator();
    String result = "";
    
    while (it.hasNext()) {
      result += "<" + resourceName + "><id>"+ it.next() + "</id></" + resourceName + ">";
    }
    
    return XMLDeclaration + "<" + resourceName + "s type=\"array\">" + result + "</" + resourceName + "s>";
  }

  
}
