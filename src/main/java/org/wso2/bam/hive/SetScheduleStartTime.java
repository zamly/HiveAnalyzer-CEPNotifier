package org.wso2.bam.hive;

import org.wso2.carbon.analytics.hive.extension.AnalyzerContext;
import org.wso2.carbon.analytics.hive.extension.HiveAnalyzer;

import java.util.Date;

/**
 * Created by zamly-PC on 12/11/14.
 */
public class SetScheduleStartTime implements HiveAnalyzer {
    @Override
    public void execute(AnalyzerContext analyzerContext) {

        Date now = new Date();
        long nowTimeStamp = now.getTime();

        analyzerContext.setProperty("TIMESTAMP_SCRIPT_STARTED", String.valueOf(nowTimeStamp));
    }
}
