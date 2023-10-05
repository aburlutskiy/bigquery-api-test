package org.ab.bigquery.nonbreaking;

import com.google.cloud.bigquery.*;
import lombok.Builder;
import lombok.extern.log4j.Log4j;

import java.util.ArrayList;
import java.util.List;

@Builder
@Log4j
public class RenameColumnCommand implements Runnable {

    private final BigQuery bigquery;

    private final String dataset;
    private final String tableName;


    @Override
    public void run() {
        log.debug("Before change schema: " + getTable().getDefinition().getSchema().toString());
        log.debug("Updating...");
        updateSchema();
        log.debug("After change schema: " + getTable().getDefinition().getSchema().toString());
    }

    public void restore() {
        final Table table = getTable();
        final ExternalTableDefinition tableDefinition = (ExternalTableDefinition) table.getDefinition();
        final ExternalTableDefinition.Builder builder = tableDefinition.toBuilder();
        Schema schema = table.getDefinition().getSchema();

        // Create a new schema adding the current fields, plus the new one
        List<Field> field_list = new ArrayList<>(schema.getFields()
                .stream()
                .map(f -> f.getName().equals("c1_changed") ? f.toBuilder().setName("c1").build() : f)
                .toList());

        bigquery.update(table.toBuilder()
                .setDefinition(builder.setSchema(Schema.of(field_list))
                        .build())
                .build());
    }

    private void updateSchema() {
        final Table table = getTable();
        final ExternalTableDefinition tableDefinition = (ExternalTableDefinition) table.getDefinition();
        final ExternalTableDefinition.Builder builder = tableDefinition.toBuilder();
        Schema schema = table.getDefinition().getSchema();
        // Create a new schema adding the current fields, plus the new one
        List<Field> field_list = schema.getFields()
                .stream()
                .map(f -> f.getName().equals("c1") ? f.toBuilder().setName("c1_changed").build() : f)
                .toList();

        builder.setSchema(Schema.of(field_list));

        bigquery.update(table.toBuilder().setDefinition(builder.build()).build());
    }

    private Table getTable() {
        return bigquery.getTable(dataset, tableName);
    }
}
