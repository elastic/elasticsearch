/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import static org.elasticsearch.xpack.prelert.job.persistence.ElasticsearchJobProvider.PRELERT_USAGE_INDEX;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import org.elasticsearch.xpack.prelert.job.usage.Usage;

public class UsagePersister extends AbstractComponent {
    private static final String USAGE_DOC_ID_PREFIX = "usage-";

    private final Client client;
    private final DateTimeFormatter dateTimeFormatter;
    private final Map<String, Object> upsertMap;

    public UsagePersister(Settings settings, Client client) {
        super(settings);
        this.client = client;
        dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXX", Locale.ROOT);
        upsertMap = new HashMap<>();

        upsertMap.put(ElasticsearchMappings.ES_TIMESTAMP, "");
        upsertMap.put(Usage.INPUT_BYTES, null);
    }

    public void persistUsage(String jobId, long bytesRead, long fieldsRead, long recordsRead) {
        ZonedDateTime nowTruncatedToHour = ZonedDateTime.now().truncatedTo(ChronoUnit.HOURS);
        String formattedNowTruncatedToHour = nowTruncatedToHour.format(dateTimeFormatter);
        String docId = USAGE_DOC_ID_PREFIX + formattedNowTruncatedToHour;
        upsertMap.put(ElasticsearchMappings.ES_TIMESTAMP, formattedNowTruncatedToHour);

        // update global count
        updateDocument(jobId, PRELERT_USAGE_INDEX, docId, bytesRead, fieldsRead, recordsRead);
        updateDocument(jobId, JobResultsPersister.getJobIndexName(jobId), docId, bytesRead,
                fieldsRead, recordsRead);
    }


    /**
     * Update the metering document in the given index/id.
     * Uses a script to update the volume field and 'upsert'
     * to create the doc if it doesn't exist.
     *
     * @param jobId             The id of the job
     * @param index             the index to persist to
     * @param id                Doc id is also its timestamp
     * @param additionalBytes   Add this value to the running total
     * @param additionalFields  Add this value to the running total
     * @param additionalRecords Add this value to the running total
     */
    private void updateDocument(String jobId, String index, String id, long additionalBytes, long additionalFields,
                                long additionalRecords) {
        upsertMap.put(Usage.INPUT_BYTES, additionalBytes);
        upsertMap.put(Usage.INPUT_FIELD_COUNT, additionalFields);
        upsertMap.put(Usage.INPUT_RECORD_COUNT, additionalRecords);

        logger.trace("[{}] ES API CALL: upsert ID {} type {} in index {} by running painless script update-usage with " +
                "arguments bytes={} fieldCount={} recordCount={}", jobId, id, Usage.TYPE, index, additionalBytes,
                additionalFields, additionalRecords);

        try {
            ElasticsearchScripts.upsertViaScript(client, index, Usage.TYPE, id,
                    ElasticsearchScripts.newUpdateUsage(additionalBytes, additionalFields,
                            additionalRecords),
                    upsertMap);
        } catch (VersionConflictEngineException e) {
            logger.error(new ParameterizedMessage("[{}] Failed to update the Usage document [{}] in index [{}]",
                    new Object[]{jobId, id, index}, e));
        }
    }
}
