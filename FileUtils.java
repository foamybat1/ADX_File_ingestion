package com.embibe.incidentmanagementsystem.utils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;

import static com.microsoft.azure.kusto.ingest.IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES;
import static com.microsoft.azure.kusto.ingest.IngestionProperties.IngestionReportMethod.QUEUE_AND_TABLE;

/**
 * Example to demonstrate Data Explorer ingestion and management operations
 */
@Log4j2
public class FileUtils {
    static String tenantIDEnvVar = "adx.tenantid";
    static String clientIDEnvVar = "AZURE_SP_CLIENT_ID";
    static String clientSecretEnvVar = "AZURE_SP_CLIENT_SECRET";
    static String endpointEnvVar = "KUSTO_ENDPOINT";
    static String databaseEnvVar = "KUSTO_DB";

    static String tenantID = "e80cb695-9b66-4414-8d04-6b78c3fb0f9c";
    static String clientID = "84c105f1-ff49-4d01-b010-995f9a4899fb";
    static String clientSecret = "Njo8Q~hA-HFBuR57lOoU2YdPz85x7JBr7NO.Ia4x";
    static String endpoint = "https://embibedataadx.jioindiawest.kusto.windows.net/";
    static String database = "incidentmanagement";

    public static void main(final String[] args) throws Exception {
        //dropTable(database);
        //createTable(database);
        //createMapping(database);
        ingestFile(database,null);
    }

    /**
     * validates whether required environment variables are present
     */
    static {
        tenantID = "e80cb695-9b66-4414-8d04-6b78c3fb0f9c";
        if (tenantID == null) {
            throw new IllegalArgumentException("Missing environment variable " + tenantIDEnvVar);
        }

        clientID = "623b8ff6-7393-430a-8890-15f372445fcc";
        if (clientID == null) {
            throw new IllegalArgumentException("Missing environment variable " + clientIDEnvVar);
        }

        clientSecret = "gUx8Q~wexlLI.Y3rc.PRnplDk.r8qMu07l6QOaTo";
        if (clientSecret == null) {
            throw new IllegalArgumentException("Missing environment variable " + clientSecretEnvVar);
        }

        endpoint = "https://embibepreprodadx.jioindiawest.kusto.windows.net/";
        if (endpoint == null) {
            throw new IllegalArgumentException("Missing environment variable " + endpointEnvVar);
        }

        database = "incidentmanagement";
        if (database == null) {
            throw new IllegalArgumentException("Missing environment variable " + databaseEnvVar);
        }

    }

    /**
     * creates a Client object
     *
     * @return Client object to execute control and query operations
     * @throws Exception
     */
    static Client getClient() throws Exception {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(endpoint, clientID,
                clientSecret, tenantID);

        return ClientFactory.createClient(csb);
    }

    // ifexists can also be added e.g. .drop table StormEvents ifexists
    static String dropTableCommand = ".drop table StormEvents";

    /**
     * drops a table from the database
     *
     * @param database name of the kusto database
     */
    static void dropTable(String database) {
        try {
            getClient().execute(database, dropTableCommand);
        } catch (Exception e) {
            System.out.println("Failed to drop table: " + e.getMessage());
        }
        System.out.println("Table dropped");
    }

    static final String createTableCommand = ".create table cloudflare_test ( clientIP: string, originIP: string, clientRequestReferer: string, requestEpoch: long, responseEpoch: long, requestDate: date, responseDate: date, clientRequestURI: string, requestPath: string, parameters: string, clientRequestHost: string, requestEnv: string, edgeResponseStatus: int, clientRequestMethod: string, originResponseStatus: int, originResponseDurationMs: long, clientRequestUserAgent: string, clientRequestProtocol: string, requestHeaders: dynamic, responseHeaders: dynamic, userId: string)";

    /**
     * creates table in a database. to validate, run .show table StormEvents
     *
     * @param database name of the kusto database
     */
    static void createTable(String database) {
        try {
            getClient().execute(database, createTableCommand);
        } catch (Exception e) {
            System.out.println("Failed to create table: " + e.getMessage());
            return;
        }
        System.out.println("Table created");
    }

    static final String createMappingCommand = ".create table cloudflare_test ingestion csv mapping 'cloudflare_test_CSV_Mapping' '[{\"Name\":\"rayId\",\"datatype\":\"string\",\"Ordinal\":0}, {\"Name\":\"clientIP\",\"datatype\":\"string\",\"Ordinal\":1},{\"Name\":\"originIP\",\"datatype\":\"string\",\"Ordinal\":2},{\"Name\":\"clientRequestReferer\",\"datatype\":\"string\",\"Ordinal\":3},{\"Name\":\"requestEpoch\",\"datatype\":\"long\",\"Ordinal\":4},{\"Name\":\"responseEpoch\",\"datatype\":\"long\",\"Ordinal\":5},{\"Name\":\"requestDate\",\"datatype\":\"date\",\"Ordinal\":6},{\"Name\":\"responseDate\",\"datatype\":\"date\",\"Ordinal\":7},{\"Name\":\"clientRequestURI\",\"datatype\":\"string\",\"Ordinal\":8},{\"Name\":\"requestPath\",\"datatype\":\"string\",\"Ordinal\":9},{\"Name\":\"parameters\",\"datatype\":\"string\",\"Ordinal\":10},{\"Name\":\"clientRequestHost\",\"datatype\":\"string\",\"Ordinal\":11},{\"Name\":\"requestEnv\",\"datatype\":\"string\",\"Ordinal\":12},{\"Name\":\"edgeResponseStatus\",\"datatype\":\"int\",\"Ordinal\":13},{\"Name\":\"clientRequestMethod\",\"datatype\":\"string\",\"Ordinal\":14},{\"Name\":\"originResponseStatus\",\"datatype\":\"int\",\"Ordinal\":15},{\"Name\":\"originResponseDurationMs\",\"datatype\":\"long\",\"Ordinal\":16},{\"Name\":\"clientRequestUserAgent\",\"datatype\":\"string\",\"Ordinal\":17},{\"Name\":\"clientRequestProtocol\",\"datatype\":\"string\",\"Ordinal\":18},{\"Name\":\"requestHeaders\",\"datatype\":\"dynamic\",\"Ordinal\":19},{\"Name\":\"responseHeaders\",\"datatype\":\"dynamic\",\"Ordinal\":20},{\"Name\":\"userId\",\"datatype\":\"string\",\"Ordinal\":21}]'";

    /**
     * create a mapping reference. to validate, run .show table StormEvents
     * ingestion mappings
     *
     * @param database
     */
    static void createMapping(String database) {
        try {
            getClient().execute(database, createMappingCommand);
        } catch (Exception e) {
            log.info("Failed to create mapping: " + e.getMessage());
            return;
        }
        log.info("Mapping created");
    }

    static IngestClient getIngestionClient() throws Exception {
        String ingestionEndpoint = "https://ingest-" + URI.create(endpoint).getHost();
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(ingestionEndpoint,
                clientID, clientSecret, tenantID);

        return IngestClientFactory.createClient(csb);
    }

    static final String tableName = "cloudflare_test";
    static final String ingestionMappingRefName = "cloudflare_test_CSV_Mapping";
    /**
     * queues ingestion to Azure Data Explorer and waits for it to complete or fail
     *
     * @param database name of the kusto database
     * @throws InterruptedException
     */
    public static void ingestFile(String database, ByteArrayInputStream byteArrayInputStream) throws InterruptedException, IOException {
//        String blobPath = String.format(blobStorePathFormat, blobStoreAccountName, blobStoreContainer,
//                blobStoreFileName, blobStoreToken);
//        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath);
//        byte[] bytes = Files.readAllBytes(Path.of("/Users/sunny/Documents/work_repo/incident-management-system/src/main/resources/data/cloudflare_sample.csv"));
//        FileSourceInfo blobSourceInfo = new FileSourceInfo("/Users/sunny/Documents/work_repo/incident-management-system/src/main/resources/data/cloudflare_sample.csv",bytes.length);
        IngestionProperties ingestionProperties = new IngestionProperties(database, tableName);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);
        ingestionProperties.setIngestionMapping(ingestionMappingRefName, IngestionMappingKind.CSV);
        ingestionProperties.setReportLevel(FAILURES_AND_SUCCESSES);
        ingestionProperties.setReportMethod(QUEUE_AND_TABLE);

        CountDownLatch ingestionLatch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                IngestionResult result = null;
                try {
                    ;
                    //result = getIngestionClient().ingestFromFile(blobSourceInfo, ingestionProperties);
                    result = getIngestionClient().ingestFromStream(new StreamSourceInfo(byteArrayInputStream),ingestionProperties);
                } catch (Exception e) {
                   log.info("Failed to initiate ingestion: " + e.getMessage());
                    ingestionLatch.countDown();
                }
                try {
                    IngestionStatus status = result.getIngestionStatusCollection().get(0);
                    while (status.status == OperationStatus.Pending) {
                        Thread.sleep(5000);
                        status = result.getIngestionStatusCollection().get(0);
                    }
                    log.info("Ingestion completed");
                    log.info("Final status: " + status.status);
                    ingestionLatch.countDown();
                } catch (Exception e) {
                    log.info("Failed to get ingestion status: " + e.getMessage());
                    ingestionLatch.countDown();
                }
            }

        }).start();

        log.info("Waiting for ingestion to complete...");
        ingestionLatch.await();
    }
}
