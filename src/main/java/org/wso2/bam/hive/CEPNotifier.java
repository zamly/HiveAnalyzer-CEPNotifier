package org.wso2.bam.hive;

import org.wso2.carbon.analytics.hive.extension.AnalyzerContext;
import org.wso2.carbon.analytics.hive.extension.HiveAnalyzer;
import org.wso2.carbon.databridge.agent.thrift.Agent;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.exception.*;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Date;

/**
 * Created by zamly-PC on 12/11/14.
 */
public class CEPNotifier implements HiveAnalyzer {

    public static String STREAM_NAME = "";
    public static String VERSION = "";
    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(CEPNotifier.class);

    @Override
    public void execute(AnalyzerContext analyzerContext) {

        String timestampNow = analyzerContext.getProperty("TIMESTAMP_SCRIPT_STARTED");
        String timestamp = analyzerContext.getParameters().get("timestamp");

        if (!analyzerContext.getParameters().get("stream").equals(null)) {
            STREAM_NAME = analyzerContext.getParameters().get("stream");
        }

        if (!analyzerContext.getParameters().get("version").equals(null)) {
           VERSION = analyzerContext.getParameters().get("version");
        }

        //logger.info("testing......."+ timestampNow);
        long nowTimestamp = Long.parseLong(timestampNow);

        try {
            testSendingEvent(nowTimestamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testSendingEvent(long timestampNow)
            throws MalformedURLException, AuthenticationException, TransportException,
            AgentException, UndefinedEventTypeException,
            DifferentStreamDefinitionAlreadyDefinedException,
            InterruptedException,
            MalformedStreamDefinitionException,
            StreamDefinitionException {

        setTrustStoreParams();

        Agent agent = new Agent();
        DataPublisher dataPublisherCEP = new DataPublisher("tcp://localhost:7611", "admin", "admin", agent);

        String streamId = getStreamID(dataPublisherCEP);

        logger.info("1st stream defined: "+streamId);

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

        try {
            streamId = dataPublisher.findStreamId(STREAM_NAME, VERSION);
        } catch (Exception e) {

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
        }

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

