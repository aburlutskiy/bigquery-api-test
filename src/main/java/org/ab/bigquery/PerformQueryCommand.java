package org.ab.bigquery;

import com.google.cloud.bigquery.*;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Builder
@Log4j
public class PerformQueryCommand implements Runnable{

    private List<Map<String, Object>> expectedOutputs;

    private BigQuery bigquery;

    private final Map<String, Long> aggr = new HashMap<>();

    @SneakyThrows
    @Override
    public void run() {
        aggr.clear();
        TableResult result = this.performQuery(bigquery);

        // Print all pages of the results.
        for (FieldValueList row : result.iterateAll()) {
            String indicator = row.get("indicator").getStringValue();
            Long sum = row.get("s").getLongValue();
            log.debug(String.format("%s, %s", indicator, sum));
            aggr.put(indicator, sum);
        }
    }

    public Boolean isMetExpectation(){
        return expectedOutputs.stream()
                .anyMatch(aggr::equals);
    }

    private TableResult performQuery(BigQuery bigquery) throws InterruptedException {
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                                """
                                        with tq as (select *,\s
                                          row_number() over (partition by indicator ORDER BY value) as rank,
                                          sum(value) over (partition by indicator) as s
                                          from test_dataset.ml1800\s
                                        )
                                        select indicator, s
                                        from tq
                                        where rank = 1
                                        order by indicator
                                        limit 5
                                        """)
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        final JobStatistics statistics = queryJob.getStatistics();
        log.debug(String.format("Query status is %s. Duration is %s \n", queryJob.getStatus().getState().toString(), Duration.ofMillis(statistics.getEndTime() - statistics.getStartTime())));

        // Get the results.
        return queryJob.getQueryResults();
    }
}
