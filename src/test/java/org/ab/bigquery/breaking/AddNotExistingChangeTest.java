package org.ab.bigquery.breaking;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.ab.bigquery.PerformQueryCommand;
import org.junit.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static java.util.Map.entry;


public class AddNotExistingChangeTest {

    /**
     * BigQuery service instance
     */
    private BigQuery bigquery;

    /**
     * Command to update external tables
     */
    private AddNotExistingColumnCommand addNotExistingColumnCommand;

    /**
     * Command to perform queries and verify results
     */
    private PerformQueryCommand performQueryCommand;

    /**
     * Method to set up resources before running tests
     *
     * @throws IOException
     */
    @Before
    public void setUp() throws IOException {
        // Initialize the BigQuery service
        bigquery = BigQueryOptions.newBuilder()
                .build().getService();

        // Configure the command to update external tables
        addNotExistingColumnCommand = AddNotExistingColumnCommand.builder()
                .bigquery(bigquery)
                .dataset("test_dataset")
                .tableName("ml1800")
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

    @After
    public void after() {
        assert addNotExistingColumnCommand != null;
        addNotExistingColumnCommand.restore();
    }

    /**
     * Test case to simulate breaking changes don't interrupt a running query
     */
    @Test
    public void runningQueryNotFail() throws InterruptedException {
        final CompletableFuture<Void> queryFuture = CompletableFuture.runAsync(performQueryCommand);
        addNotExistingColumnCommand.run();

        queryFuture.join();

        //check whether the query has completed successfully
        Assert.assertFalse(queryFuture.isCompletedExceptionally());
        Assert.assertFalse(queryFuture.isCancelled());

        //check whether the output has met expectations
        //we expect that the output must match any from the list
        Assert.assertTrue(performQueryCommand.isMetExpectation());
    }

    /**
     * Test case to simulate breaking changes prior to a query which shoud cause a failure
     */
    @Test(expected = CompletionException.class)
    public void precedingQueryFails() throws InterruptedException {
        addNotExistingColumnCommand.run();
        final CompletableFuture<Void> queryFuture = CompletableFuture.runAsync(performQueryCommand);
        queryFuture.join();
    }


}
