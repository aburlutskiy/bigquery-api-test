package org.ab.bigquery.breaking;

import com.google.cloud.bigquery.*;
import lombok.Builder;
import lombok.extern.log4j.Log4j;

import java.util.ArrayList;
import java.util.List;

@Builder
@Log4j
public class AddNotExistingColumnCommand implements Runnable {

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
                .filter(f -> !f.getName().equals("breaking_column"))
                .toList());

        final ExternalTableDefinition externalTableDefinition = builder.setSchema(Schema.of(field_list))
                .build();

        bigquery.update(table.toBuilder().setDefinition(externalTableDefinition).build());
    }

    private void updateSchema() {
        final Table table = getTable();
        final ExternalTableDefinition tableDefinition = (ExternalTableDefinition) table.getDefinition();
        final ExternalTableDefinition.Builder builder = tableDefinition.toBuilder();
        Schema schema = table.getDefinition().getSchema();
        // Create a new schema adding the current fields, plus the new one
        List<Field> field_list = new ArrayList<>(schema.getFields()
                .stream()
                .toList());

        field_list.add(Field.of("breaking_column", LegacySQLTypeName.STRING));

        final ExternalTableDefinition externalTableDefinition = builder.setSchema(Schema.of(field_list))
                .build();

        bigquery.update(table.toBuilder().setDefinition(externalTableDefinition).build());
    }

    private Table getTable() {
        return bigquery.getTable(dataset, tableName);
    }
}
