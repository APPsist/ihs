package de.appsist.service.ihs;
/*
 * Copyright 2013 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.platform.Verticle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.appsist.commons.misc.StatusSignalConfiguration;
import de.appsist.commons.misc.StatusSignalSender;

/*

 */
public class IHSMainVerticle
    extends Verticle
{

    // JsonObject representing the configuration of this verticle
    private JsonObject config;
    // routematcher to match http queries
    private BasePathRouteMatcher routeMatcher = null;
    // verticle logger
    private static final Logger log = LoggerFactory.getLogger(IHSMainVerticle.class);

    // needed prefix string to build SparQL queries
    final private String prefixString = "PREFIX app: <http://www.appsist.de/ontology/> PREFIX terms: <http://purl.org/dc/terms/>";

    // holds allowed content types
    private final List<String> contentTypes = new ArrayList<String>();

    // basePath String
    private String basePath = "";

    // eventbus prefix
    private final String eventbusPrefix = "appsist:";

    // step id label map
    private Map<String, String> stepIdLabelMap ;
    
    @Override
  public void start() {

        // ensure verticle is configured
        if (container.config() != null && container.config().size() > 0) {
            config = container.config();
        }
        else {
            // container.logger().warn("Warning: No configuration applied! Using default settings.");
            config = getDefaultConfiguration();
        }
        contentTypes.add("task");
        contentTypes.add("additional");
        contentTypes.add("activity");

        this.basePath = config.getObject("webserver").getString("basePath");
        // init SparQL prefix string


        initializeHttpRequestHandlers();
        vertx.setTimer(2000, new Handler<Long>()
        {

            @Override
            public void handle(Long arg0)
            {
                //log.info("Initializing stepId Label Map");
                // UserManager um = UserManager.getInstance();
                initStepIdLabelMap();
            }

        });
        
        
        log.info("*******************");
        log.info("  Inhalteselektor auf Port "
                + config.getObject("webserver").getNumber("port") + " gestartet ");
        log.info("                              *******************");
        
        JsonObject statusSignalObject = config.getObject("statusSignal");
        StatusSignalConfiguration statusSignalConfig;
        if (statusSignalObject != null) {
          statusSignalConfig = new StatusSignalConfiguration(statusSignalObject);
        } else {
          statusSignalConfig = new StatusSignalConfiguration();
        }

        StatusSignalSender statusSignalSender =
          new StatusSignalSender("ihs", vertx, statusSignalConfig);
        statusSignalSender.start();


  }


    private void initStepIdLabelMap() {
		// create new Map
    	this.stepIdLabelMap = new HashMap<String,String>();
		// request labels from Information Workbench
    	String labelForStepQuery = "PREFIX app: <http://www.appsist.de/ontology/> SELECT DISTINCT ?uri ?label WHERE {?class rdfs:subClassOf* app:Prozesselement . ?uri a ?class . ?uri rdfs:label ?label FILTER(LANGMATCHES(LANG(?label), 'de'))}";
    	JsonObject message = new JsonObject();
    	JsonObject sQuery = new JsonObject();
        sQuery.putString("query", labelForStepQuery);
        message.putObject("sparql", sQuery);
    	vertx.eventBus().send(this.eventbusPrefix+"requests:semwiki", message,new Handler<Message<String>>(){

			@Override
			public void handle(Message<String> queryResult) {
				JsonObject queryResultObject = new JsonObject(queryResult.body());
				JsonArray queryResults = queryResultObject.getObject("results").getArray("bindings");
				for (Object result:queryResults){
					if (result instanceof JsonObject){
						JsonObject resultObject = (JsonObject) result;
						String uriString = resultObject.getObject("uri").getString("value","");
						// canonicalize URIs
						String[] uriStringSplitArray = uriString.split("/");
						String canonicalURIString = uriStringSplitArray[uriStringSplitArray.length-2]+"/"+uriStringSplitArray[uriStringSplitArray.length-1];
							
						String labelString = resultObject.getObject("label").getString("value","");
						// fill stepIdLabelMap
						stepIdLabelMap.put(canonicalURIString, labelString);
					}
				}
				
			}
    	});
	}
    
    private String getLabelForStepId(String stepId){
    	if (stepIdLabelMap.keySet().contains(stepId)){
    		return stepIdLabelMap.get(stepId);
    	}
    	
    	return "Unbekannter Schritt";
    }

	private void initializeHttpRequestHandlers()
    {
        // init routematcher with basePath from configuration
        routeMatcher = new BasePathRouteMatcher(this.basePath);
        // set handlers here

        final String staticFileDirectory = config.getObject("webserver").getString("statics");

        routeMatcher.get("/contentForTask", new Handler<HttpServerRequest>()
        {
            @Override
            public void handle(final HttpServerRequest request)
            {
                findFullMeasureId(request, "task");
                // loadTaskContentsForUser(request);
            }
        });
        

        routeMatcher.get("/contentForActivity", new Handler<HttpServerRequest>()
        {
            @Override
            public void handle(final HttpServerRequest request)
            {
                findFullMeasureId(request, "activity");
                // loadActivityContentsForUser(request);
            }
        });

        routeMatcher.get("/additionalContent", new Handler<HttpServerRequest>()
        {
            @Override
            public void handle(final HttpServerRequest request)
            {
                findFullMeasureId(request, "additional");
                // loadTaskContentsForUser(request);
            }
        });

        /*
         * This entry serves files from a directory specified in the configuration. In the
         * default configuration, the files are served from "src/main/resources/www", which is
         * packaged with the module.
         */
        routeMatcher.getWithRegEx("/.*", new Handler<HttpServerRequest>()
        {

            @Override
            public void handle(HttpServerRequest request)
            {
                request.response().sendFile(staticFileDirectory + request.path());
            }
        });

        // start verticle webserver at configured port
        vertx.createHttpServer().requestHandler(routeMatcher)
                .listen(config.getObject("webserver").getInteger("port"));

    }

    // find full URI of measure ID
    private void findFullMeasureId(final HttpServerRequest request, final String contentType)
    {
        if (!contentTypes.contains(contentType)) {
            // if unknown content type return empty content id and stop
            request.response().end(new JsonObject().encode());
            return;
        }

        log.debug("contents request: " + request.path() + " | " + request.query());
        JsonObject message = new JsonObject();
        final String measureId = request.params().get("measureId");
        final String elementId = request.params().get("elementId");
        final String userId = request.params().get("userId");

        String sparqlQueryForMeasureId = this.prefixString
 + " SELECT DISTINCT ?uri WHERE { ?uri a ?_ FILTER (REGEX(str(?uri),'" + measureId + "$')) }";
        JsonObject sQuery = new JsonObject();
        sQuery.putString("query", sparqlQueryForMeasureId);
        message.putObject("sparql", sQuery);
        vertx.eventBus().send(this.eventbusPrefix + "requests:semwiki", message,
                new Handler<Message<String>>()
                {
                    public void handle(Message<String> reply)
                    {
                        List<String> foundMeasureIds = new ArrayList<String>();
                        try {
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode root = mapper.readTree(reply.body());
                            if (null != root) {
                                foundMeasureIds = root.findValuesAsText("value");
                            }

                        }

                        catch (Exception e) {
                            e.printStackTrace();
                        }
                        if (!foundMeasureIds.isEmpty()) {
                            String fullMeasureId = foundMeasureIds.get(0);
                    switch (contentType) {
                        case "task" :
                            loadTaskContentsForUser(fullMeasureId, elementId, userId, request);
                            break;
                        case "activity" :
                            loadActivityContentsForUser(fullMeasureId, userId, request);
                            break;
                        case "additional" :
                            getUserInformation(fullMeasureId + "/" + elementId, userId, request);
                    }

                        } else{
                        	request.response().end(new JsonObject().encode());
                        }

                    };

                });


    }

    // we need some information about the user
    // contact usermodel service

    private void getUserInformation(final String measureId, String userId, final HttpServerRequest request)
    {
        JsonObject infoRequest = new JsonObject();
        infoRequest.putString("sid", "sessionId");
        infoRequest.putString("userId", userId);
        infoRequest.putString("token", "token");
        Handler<Message<JsonObject>> userInformationHandler = new Handler<Message<JsonObject>>()
        {

            @Override
            public void handle(Message<JsonObject> message)
            {
                
            	JsonObject messageBody = message.body();
            	if (messageBody.toMap().size() > 0){
                    processUserInformation(messageBody, measureId, request);
                } else {
                    request.response().end(new JsonObject().encode());
                } 
            	

            }

        };
        vertx.eventBus().send("appsist:service:usermodel#getUserInformation", infoRequest, userInformationHandler);
    }

    private void processUserInformation(JsonObject messageBody, String measureId, HttpServerRequest request)
    {
        JsonObject userInformation = messageBody.getObject("userInformation");
        
        // store information about user in corresponding maps
        String employeeType = userInformation.getString("employeeType", null);
        if (null != employeeType && !"".equals(employeeType)) {
            loadAdditionalContentsForUser(measureId, employeeType, request);
        } else {
            request.response().end(new JsonObject().encode());
        }
    }
    
    // for one user get list with all cleared measures
    private void loadTaskContentsForUser(String measureId, String elementId, String userId,
            final HttpServerRequest request)
    {

            //log.info("loadTaskContents request: " + request.path() + " | " + request.query());
        JsonObject message = new JsonObject();
        final String taskId = measureId + "/" + elementId;
        final String bpmnStepId = measureId.substring(measureId.lastIndexOf("/")+1) + "/" + elementId;
        sendCurrentStepToKVD(userId, bpmnStepId);
        String sparqlQueryForContents = this.prefixString
                + " SELECT DISTINCT ?inhalt WHERE {?inhalt app:informiertUeber <"
                + taskId + "> . ?inhalt rdf:type app:Instruktion } ";
        //log.info("taskcontent: "+ sparqlQueryForContents);
        JsonObject sQuery = new JsonObject();
        sQuery.putString("query", sparqlQueryForContents);
        message.putObject("sparql", sQuery);
        vertx.eventBus().send(this.eventbusPrefix + "requests:semwiki", message,
                new Handler<Message<String>>()
                {
                    public void handle(Message<String> reply)
                    {
                        try {
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode root = mapper.readTree(reply.body());
                            if (null != root) {
                                List<String> valueList = root.findValuesAsText("value");
                                if (null != valueList && !valueList.isEmpty()) {
                                    JsonObject cId = new JsonObject();

                                    String resultString = valueList.get(0);
                                    resultString = resultString.substring(resultString
                                            .lastIndexOf("/") + 1);
                                    cId.putString("contentId", resultString);
                                    request.response().end(cId.encode());
                                }
                                else {
                                    request.response().end(new JsonObject().encode());
                                }
                            }
                            else {
                                request.response().end(new JsonObject().encode());
                            }

                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    };
                });

    }

    // for one user get list with all cleared measures
    private void loadActivityContentsForUser(String measureId, String userId,
            final HttpServerRequest request)
    {
       // log.debug("loadActivityContents request: " + request.path() + " | "
          //      + request.query());
        JsonObject message = new JsonObject();
        final String calledProcessId = request.params().get("calledProcess");
        final String taskId = measureId + "/" + calledProcessId;
        final String bpmnStepId = measureId.substring(measureId.lastIndexOf("/")+1) + "/" + calledProcessId;
        sendCurrentStepToKVD(userId, bpmnStepId);
        String sparqlQueryForContents = this.prefixString
                + " SELECT DISTINCT ?inhalt WHERE {?inhalt app:informiertUeber <"
                + taskId + "> . ?inhalt rdf:type app:Instruktion } ";
        //log.info("Activity: "+sparqlQueryForContents);
        JsonObject sQuery = new JsonObject();
        sQuery.putString("query", sparqlQueryForContents);
        message.putObject("sparql", sQuery);
        vertx.eventBus().send(this.eventbusPrefix + "requests:semwiki", message,
                new Handler<Message<String>>()
                {
                    public void handle(Message<String> reply)
                    {
                        try {
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode root = mapper.readTree(reply.body());
                            if (null != root) {
                                List<String> valueList = root.findValuesAsText("value");
                                if (null != valueList && !valueList.isEmpty()) {
                                    JsonObject cId = new JsonObject();

                                    String resultString = valueList.get(0);
                                    resultString = resultString
                                            .substring(resultString.lastIndexOf("/") + 1);
                                    cId.putString("contentId", resultString);	
                                    request.response().end(cId.encode());
                                }
                                else {
                                    request.response().end(new JsonObject().encode());
                                }
                            }
                            else {
                                request.response().end(new JsonObject().encode());
                            }

                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    };
                });

    }

    private void sendCurrentStepToKVD(String userId, String bpmnStepId) {
		// TODO Auto-generated method stub
    	JsonObject stepIdMessage = new JsonObject();
    	String stepLabel = getLabelForStepId(bpmnStepId);
    	if (!stepLabel.equals("Unbekannter Schritt")){
    		
    		stepIdMessage.putString("userId", userId);
        	stepIdMessage.putString("stepId", bpmnStepId);
        	stepIdMessage.putString("stepLabel", stepLabel);
    		vertx.eventBus().publish("appsist:services:kvdconnection:stepid", stepIdMessage);
    		//log.info("sending stepid with message: "+ stepIdMessage.encodePrettily());
    	} else {
    		log.info("Unknown step :"+ bpmnStepId);
    	}
	}


    private void loadAdditionalContentsForUser(String processIds, String stelle, final HttpServerRequest request)
    {
    	processIds = "{<"+processIds+">}";
        JsonObject message = new JsonObject();
        String sparqlQueryForContents = this.prefixString + " SELECT DISTINCT ?inhalt ?vorschau WHERE {VALUES ?p " + processIds + " ?inhalt app:informiertUeber ?p FILTER ((NOT EXISTS {?inhalt rdf:type app:Instruktion}) && ((NOT EXISTS {?inhalt app:hatZielgruppe ?_}) || EXISTS {?inhalt app:hatZielgruppe <"+ stelle +">}))"
        		+ " OPTIONAL {?inhalt app:hasPreview ?vorschau}}";
        JsonObject sQuery = new JsonObject();
        sQuery.putString("query", sparqlQueryForContents);
        message.putObject("sparql", sQuery);
        vertx.eventBus().send(this.eventbusPrefix + "requests:semwiki", message, new Handler<Message<String>>()
        {
            public void handle(Message<String> reply)
            {
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode root = mapper.readTree(reply.body());
                    root = root.get("results").get("bindings").findValue("inhalt");
                    if (null != root) {
                        List<String> valueList = root.findValuesAsText("value");
                        if (null != valueList && !valueList.isEmpty()) {
                            JsonObject cId = new JsonObject();

                            String resultString = valueList.get(0);
                            resultString = resultString.substring(resultString.lastIndexOf("/") + 1);
                            cId.putString("contentId", resultString);
                            request.response().end(cId.encode());
                        }
                        else {
                            request.response().end(new JsonObject().encode());
                        }
                    }
                    else {
                        request.response().end(new JsonObject().encode());
                    }

                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            };
        });

    }


    /**
     * Create a configuration which is used if no configuration is passed to the module.
     * 
     * @return Configuration object.
     */
    private static JsonObject getDefaultConfiguration()
    {
        JsonObject defaultConfig = new JsonObject();
        JsonObject webserverConfig = new JsonObject();
        webserverConfig.putNumber("port", 7086);
        webserverConfig.putString("basePath", "/services/ihs");
        // TODO: test statics with relative path
        // until now only full path is working

        webserverConfig.putString("statics",
                "/Users/midi01/Work/svn_repositories/AppSist-svn/dfki/ihs/src/main/resources/www");

        defaultConfig.putObject("webserver", webserverConfig);

        JsonObject sparqlConfig = new JsonObject();
        sparqlConfig.putString("reqBaseUrl", "localhost");
        sparqlConfig.putString("reqPath", "/sparql");
        sparqlConfig.putNumber("reqBasePort", 8443);
        sparqlConfig.putString("ontologyPrefix", "app:");
        sparqlConfig.putString("ontologyUri", "http://www.appsist.de/ontology/");
        defaultConfig.putObject("sparql", sparqlConfig);
        return defaultConfig;
    }

}
