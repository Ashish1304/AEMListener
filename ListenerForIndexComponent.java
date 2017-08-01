package com.adobe.aem.events.replication;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import org.apache.sling.jcr.api.SlingRepository;
import org.apache.felix.scr.annotations.Reference;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import com.adobe.aem.core.SolrSearchService;
import com.adobe.aem.core.SolrServerConfiguration;

/**
 * This is a AEM component that listens to event happening to/on nodes in a given directory.
 * Based on the event type it triggers solr index or removal of index of the page
 */
@Component(immediate=true)
@Service
public class ListenerForIndexComponent implements EventListener {

	@Reference
	private SolrServerConfiguration solrConfigurationService;

	@Reference
	private SlingRepository repository;

	@Reference
	private SolrSearchService solrSearchService;

	@Reference
	private ResourceResolverFactory resolverFactory;

	private Session session;

	private ObservationManager observationManager;

	//map of <Event_type_code,Event_type_name>
	private static Map<Integer,String> event_types;

	private Logger logger = LoggerFactory.getLogger(ListenerForIndexComponent.class);

	//Logic to define the AEM Custom Event Handler
	protected void activate(ComponentContext ctx) throws RepositoryException{
		try
		{
			logger.info("Activating Listner component for solr index management");   

			session = repository.loginAdministrative(null);

			// Setup the event handler to respond to a new claim under content/claim.... 
			observationManager = session.getWorkspace().getObservationManager();

			final String path = "/content/tcs"; // define the path to listen on

			observationManager.addEventListener(this, Event.NODE_ADDED|Event.NODE_MOVED|Event.NODE_REMOVED|Event.PROPERTY_ADDED|Event.PROPERTY_CHANGED|Event.PROPERTY_REMOVED,
					path, true, null, null, false);

			event_types=new HashMap<Integer, String>();

			logger.info("Observing Node changes at path {}",path);

			event_types.put(Event.NODE_ADDED, "NODE_ADDED");
			event_types.put(Event.NODE_MOVED, "NODE_MOVED");
			event_types.put(Event.NODE_REMOVED, "NODE_REMOVED");;
			event_types.put(Event.PROPERTY_ADDED, "PROPERTY_ADDED");
			event_types.put(Event.PROPERTY_CHANGED, "PROPERTY_CHANGED");
			event_types.put(Event.PROPERTY_REMOVED, "PROPERTY_REMOVED");

		}
		catch(Exception e)
		{
			e.printStackTrace(); 
		}
	}

	protected void deactivate(ComponentContext componentContext) throws RepositoryException {

		if(observationManager != null) {
			observationManager.removeEventListener(this);
		}
		if (session != null) {
			session.logout();
			session = null;
		}
	}


	public void onEvent(EventIterator events) {

		//for each page there are many events triggered.
		//The type will be same but for each component of the page the event gets triggered
		//So its better to take the page url from any one of the events and ignore the rest
		Event event=events.nextEvent();

		String URL=createSolrUrl();
		try {
			logger.debug("~~!!~Event Details~!!~~");
			logger.debug("~~ event date ~~"+event.getDate());
			logger.debug("~~ event identifier ~~"+event.getIdentifier());
			logger.debug("~~ event path ~~"+event.getPath());
			logger.debug("~~ event type"+event_types.get(event.getType()));
			logger.debug("~~ event User Id"+event.getUserID());

			String path=event.getPath();
			if(event.getPath().indexOf("/jcr:")>0)
				path=event.getPath().substring(0, event.getPath().indexOf("/jcr:")); //just take the page path


			if(event.getType()==Event.NODE_ADDED || event.getType()==Event.PROPERTY_ADDED || event.getType()==Event.PROPERTY_CHANGED
					|| event.getType()==Event.PROPERTY_REMOVED){
				logger.info("Event {} for solr index found at {} ",event_types.get(event.getType()),path);	
				indexPageWithContent(path, "indexpages",URL);
				indexPageWithContent(path, "indexDam",URL );
			}else if(event.getType()==Event.NODE_REMOVED || event.getType()==Event.NODE_MOVED){
				logger.info("Event {} for remove solr index found at {} ",event_types.get(event.getType()),path);
				deletePageIndex(path, createSolrUrl());
			}
		} catch (RepositoryException e) {
			logger.error(e.toString());
		}                          
	}

	/**
	 * This method sends the page for indexing
	 * @param pagesResourcePath
	 * @param indexType
	 * @param URL
	 */
	private void indexPageWithContent(String pagesResourcePath, String indexType,
			String URL){

	}



	/**
	 * This method deletes the page index
	 * @param pagesResourcePath
	 * @param indexType
	 * @param URL
	 */
	private void deletePageIndex(String pagesResourcePath,String URL){
	}



	/**
	 * This method returns the solr url till the base directory
	 * 
	 * @return url
	 */
	private String createSolrUrl() {
		final String protocol = solrConfigurationService.getSolrProtocol();
		final String serverName = solrConfigurationService.getSolrServerName();
		final String serverPort = solrConfigurationService.getSolrServerPort();
		final String coreName = solrConfigurationService.getSolrCoreName();
		String URL = protocol + "://" + serverName + ":" + serverPort
				+ "/solr/" + coreName;
		return URL;
	}
}
