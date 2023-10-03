package org.ab.bigquery;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.ab.bigquery.PerformQueryCommand;
import org.ab.bigquery.UpdateExternalTableCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Map.entry;

@RunWith(Parameterized.class)
public class BigQueryUpdateTableConsistencyTest {

    /**
     * BigQuery service instance
     */
    private BigQuery bigquery;

    /**
     * Command to update external tables
     */
    private UpdateExternalTableCommand updateExternalTableCommand;

    /**
     *  Command to perform queries and verify results
     */
    private PerformQueryCommand performQueryCommand;

    @Parameterized.Parameters
    public static Object[][] data() {
        return new Object[100][0];
    }

    /**
     * Method to set up resources before running tests
     * @throws IOException
     */
    @Before
    public void setUp() throws IOException {
        // Initialize the BigQuery service
        bigquery = BigQueryOptions.newBuilder()
                .build().getService();

        // Configure the command to update external tables
        updateExternalTableCommand = UpdateExternalTableCommand.builder()
                .bigquery(bigquery)
                .dataset("test_dataset")
                .tableName("ml1800")
                .datasetFirstUri1("gs://intuit-sandbox-datalake/test_dataset/ml1800/*")
                .datasetFirstUri2("gs://intuit-sandbox-datalake/test_dataset/ml1800_2/*.csv")
                .build();

        // Configure the command to perform queries and specify expected outputs
        performQueryCommand = PerformQueryCommand.builder()
                .bigquery(bigquery)
                .expectedOutputs(List.of(Map.ofEntries(
                entry("PRCP", 58460000L),
                entry("TMAX", 1058360000L),
                entry("TMIN", 663190000L)
        ), Map.ofEntries(
                entry("PRCP", 5846L),
                entry("TMAX", 105836L),
                entry("TMIN", 66319L)
        ))).build();
    }

    /**
     * Test case to simulate changes in external tables during a query
      */
    @Test
    public void eitherBeforeOrAfterChange() throws InterruptedException {
        final CompletableFuture<Void> queryFuture = CompletableFuture.runAsync(performQueryCommand);
        while(!queryFuture.isDone()){
            //change a location in external table
            updateExternalTableCommand.run();
            Thread.sleep(2000);
        }

        //check whether the query has completed successfully
        Assert.assertFalse(queryFuture.isCompletedExceptionally());
        Assert.assertFalse(queryFuture.isCancelled());

        //check whether the output has met expectations
        //we expect that the output must match any from the list
        Assert.assertTrue(performQueryCommand.isMetExpectation());
    }
}
