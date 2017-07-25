/**
 *
 */
package com.tagtraum.perf.gcviewer.exp.impl;

/**
 * Write Summary Information in CSV format.
 * <p>
 * Write each summary metric on its own line with {@literal "tag; value; unit"} format.
 *
 * @author sean
 *
 */
public class InfluxSummaryExportFormatter implements ISummaryExportFormatter {

    public static final String GC_LOG_FILE = "gcLogFile";
    public static final String GC_TIME_SERIES = "gcTimeSeries";
    public static final String APP_TAG = "app=";
    public static final String SPACE = " ";

    private String separator = ",";
    private String gcLog = null;
    

    /**
     * @see ISummaryExportFormatter#formatLine(String, String, String)
     */
    @Override
    public String formatLine(String tag, String value, String units) {
        if (GC_LOG_FILE.equals(tag)){
            gcLog = tag;
            return null;
        }
        return GC_TIME_SERIES + separator + APP_TAG + gcLog + SPACE + tag + "=" + value + SPACE + System.currentTimeMillis();
    }
}
