package org.ab.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.extern.log4j.Log4j;

import java.util.List;
import java.util.Objects;

@Builder
@Log4j
public class UpdateExternalTableCommand implements Runnable{

    private final BigQuery bigquery;

    private final String dataset;
    private final String tableName;

    private final String datasetFirstUri1;
    private final String datasetFirstUri2;

    @Override
    public void run() {
        final String beforeUris = getTableSourceURIs();
        log.debug("Before change URIs: " + beforeUris);
        List<String> newURIs = beforeUris.contains(datasetFirstUri1) ? ImmutableList.of(datasetFirstUri2) : ImmutableList.of(datasetFirstUri1);
        log.debug("Updating...");
        updateSourceURI(newURIs);
        log.debug("After change URIs: " + getTableSourceURIs());
    }

    private void updateSourceURI(List<String> newURIs) {
        final Table table = getTable();
        final ExternalTableDefinition tableDefinition = (ExternalTableDefinition) table.getDefinition();
        final ExternalTableDefinition.Builder builder = tableDefinition.toBuilder();
        final ExternalTableDefinition externalTableDefinition = builder.setSourceUris(newURIs)
                .build();

        bigquery.update(table.toBuilder().setDefinition(externalTableDefinition).build());
    }

    private String getTableSourceURIs() {
        final ExternalTableDefinition tableDefinition = (ExternalTableDefinition) getTable().getDefinition();
        return String.join(",", Objects.requireNonNull(tableDefinition.getSourceUris()));
    }

    private Table getTable() {
        return bigquery.getTable(dataset, tableName);
    }
}
