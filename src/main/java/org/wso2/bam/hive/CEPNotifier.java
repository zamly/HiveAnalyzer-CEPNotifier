/*
*  Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.bam.hive;

import org.wso2.carbon.analytics.hive.extension.AnalyzerContext;
import org.wso2.carbon.analytics.hive.extension.HiveAnalyzer;
import org.wso2.carbon.databridge.agent.thrift.Agent;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.exception.*;

import java.io.File;
import java.net.MalformedURLException;

import org.apache.log4j.Logger;


public class CEPNotifier implements HiveAnalyzer {

    /** Stream name */
    public static String STREAM_NAME = "";

    /** Stream version */
    public static String VERSION = "";

    /** Host address of CEP */
    public static String cepHost = "localhost";

    /** Port of CEP */
    public static String cepPort = "7611";

    public static String username = "admin";
    public static String password = "admin";

    private static Logger logger = Logger.getLogger(CEPNotifier.class);

    @Override
    public void execute(AnalyzerContext analyzerContext) {

        String timestampNow = analyzerContext.getProperty("TIMESTAMP_SCRIPT_STARTED");
        //String timestamp = analyzerContext.getParameters().get("timestamp");

        // checks if the stream parameter is null
        if (!analyzerContext.getParameters().get("stream").equals("")) {
            STREAM_NAME = analyzerContext.getParameters().get("stream");
        } else {
            logger.error("Parameter stream required.");
        }

        // checks if the version parameter is null
        if (!analyzerContext.getParameters().get("version").equals("")) {
           VERSION = analyzerContext.getParameters().get("version");
        } else {
            logger.error("Parameter version required.");
        }

        if (!analyzerContext.getParameters().get("cepHost").equals("")) {
            cepHost = analyzerContext.getParameters().get("cepHost");
        } else {
            logger.error("Parameter cepHost required.");
        }

        if (!analyzerContext.getParameters().get("cepPort").equals("")) {
            cepPort = analyzerContext.getParameters().get("cepPort");
        } else {
            logger.error("Parameter cepPort required.");
        }

        if (!analyzerContext.getParameters().get("username").equals("")) {
            username = analyzerContext.getParameters().get("username");
        } else {
            logger.error("Parameter username required.");
        }

        if (!analyzerContext.getParameters().get("password").equals("")) {
            password = analyzerContext.getParameters().get("password");
        } else {
            logger.error("Parameter password required.");
        }

        try {
            sendEvent(Long.parseLong(timestampNow));
        } catch (Exception e) {
            logger.error("Event Sending Error: "+ e.getMessage());
        }
    }


    /**
     * Method sends the timestamp as an event to
     * the specified thrift endpoint.
     *
     * @param timestampNow
     *            current time stamp
     * @throws MalformedURLException
     *             the malformed url exception
     * @throws AgentException
     *             the agent exception
     * @throws AuthenticationException
     *             the authentication exception
     * @throws TransportException
     *             the transport exception
     * @throws MalformedStreamDefinitionException
     *             the malformed stream definition exception
     * @throws StreamDefinitionException
     *             the stream definition exception
     * @throws DifferentStreamDefinitionAlreadyDefinedException
     *             the different stream definition already defined exception
     * @throws InterruptedException
     *             the interrupted exception
     * @throws UndefinedEventTypeException
     */
    public void sendEvent(long timestampNow)
            throws MalformedURLException, AuthenticationException, TransportException,
            AgentException, UndefinedEventTypeException,
            DifferentStreamDefinitionAlreadyDefinedException,
            InterruptedException,
            MalformedStreamDefinitionException,
            StreamDefinitionException {

        setTrustStoreParams();

        //Agent agent = new Agent();
        DataPublisher dataPublisherCEP = new DataPublisher("tcp://"+cepHost+":"+cepPort, username, password);

        String streamId = getStreamID(dataPublisherCEP);

        logger.info("Stream defined: "+streamId);

        dataPublisherCEP.publish(streamId, new Object[]{"127.0.0.1"}, new Object[]{Integer.toString(1)},
                new Object[]{timestampNow});

        dataPublisherCEP.stop();
    }


    /**
     * Gets the stream id if already defined
     * or else created a new one.
     *
     * @param dataPublisher
     *            the data publisher
     * @return the stream id
     * @throws AgentException
     *             the agent exception
     * @throws MalformedStreamDefinitionException
     *             the malformed stream definition exception
     * @throws StreamDefinitionException
     *             the stream definition exception
     * @throws DifferentStreamDefinitionAlreadyDefinedException
     *             the different stream definition already defined exception
     */
    private static String getStreamID(DataPublisher dataPublisher)
            throws AgentException,
            MalformedStreamDefinitionException,
            StreamDefinitionException,
            DifferentStreamDefinitionAlreadyDefinedException {

        String streamId = null;

        streamId =
                dataPublisher.defineStream("{" +
                        "  'name':'" + STREAM_NAME + "'," +
                        "  'version':'" + VERSION + "'," +
                        "  'nickName': 'Pressure Sensing Information'," +
                        "  'description': 'Some Desc'," +
                        "  'tags':['foo', 'bar']," +
                        "  'metaData':[" +
                        "          {'name':'ipAdd','type':'STRING'}" +
                        "  ]," +
                        "  'correlationData':[" +
                        "          {'name':'correlationId','type':'STRING'}" +
                        "  ]," +
                        "  'payloadData':[" +
                        "          {'name':'timestamp','type':'LONG'}" +
                        "  ]" +
                        "}");

        return streamId;
    }


    public static void setTrustStoreParams() {
        File filePath = new File("src/main/resources");
        if (!filePath.exists()) {
            filePath = new File("resources");
        }
        String trustStore = filePath.getAbsolutePath();
        System.setProperty("javax.net.ssl.trustStore", trustStore + "/client-truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");

    }

}

