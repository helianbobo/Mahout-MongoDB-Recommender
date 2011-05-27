package com.paradigma.recommender.rest;

import java.io.StringReader;
import java.util.ArrayList;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import javax.servlet.*;

import org.slf4j.Logger;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;



/**
 * 
 * 
 * @author Alvaro Martin Fraguas - Paradigma Tecnologico
 *
 */
// TODO: Find a way to put these methods in PreferenceResource class
// (currently there is a conflict because it does not parse the .xml at the end of the creation path)
@Path(value="/recommender_preferences.xml")
public class PreferenceCreationResource {

  
  static @Context ServletContext servletContext;

  
  // Create a preference of a user
  @POST
  @Consumes(value="application/xml")
  public String createPreference(String body) {
    ArrayList<String> rawPreference = parseReceivedPreference(body);
    PreferenceResource.refreshPreference(rawPreference, true, servletContext);
    return body;
  }
  
  
  private static ArrayList<String> parseReceivedPreference (String body) {
    ArrayList<String> preference = new ArrayList<String> ();
    
    try {

      // Set up the XML parsing infrastructure from the String argument
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(body));
      Document doc = db.parse(is);

      // Get the root node: preference
      NodeList nodes = doc.getElementsByTagName("recommender-preference");
      Element preferenceElement = (Element) nodes.item(0);
      
      // Get the userID
      preference.add(getContentStringForTag("user-id", preferenceElement));
      
      // Get the itemID
      preference.add(getContentStringForTag("item-id", preferenceElement));
      
      // Get the preference value
      preference.add(getContentStringForTag("preference", preferenceElement));

    } catch (Exception e) {
      Logger log = (Logger) servletContext.getAttribute("log");
      log.error("Exception thrown while parsing a received preference: " + e.getMessage());
    }
    
    return preference;
  }

  
  private static String getContentStringForTag (String tag, Element root) {
    NodeList node = root.getElementsByTagName(tag);
    Element line = (Element) node.item(0);
    Node child = line.getFirstChild();
    if (child instanceof CharacterData) {
       CharacterData cd = (CharacterData) child;
       return cd.getData();
    }
    return "?";
  }
  
}