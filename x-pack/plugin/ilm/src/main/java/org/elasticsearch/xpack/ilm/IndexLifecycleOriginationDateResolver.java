/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.time.DateFormatter;

import java.util.regex.Pattern;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_ORIGINATION_DATE;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE;

public class IndexLifecycleOriginationDateResolver {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy.MM.dd");
    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile("^.*-.+$");

    public static Long resolveOriginationDate(IndexMetaData indexMetaData) {
        Boolean parseOriginationDate = indexMetaData.getSettings().getAsBoolean(LIFECYCLE_PARSE_ORIGINATION_DATE, false);
        if (parseOriginationDate) {
            String indexName = indexMetaData.getIndex().getName();
            if (INDEX_NAME_PATTERN.matcher(indexName).matches()) {
                int dateIndex = indexName.lastIndexOf("-");
                assert dateIndex != -1 : "no separator '-' found";
                String dateAsString = indexName.substring(dateIndex + 1);
                try {
                    return DATE_FORMATTER.parseMillis(dateAsString);
                } catch (ElasticsearchParseException | IllegalArgumentException e) {
                    // TODO fail?
                    return -1L;
                }
            } else {
                // TODO fail?
                return -1L;
            }
        } else {
            // TODO This should probably be the other way around, namely an explicit origination date would override the index name parsing
            return indexMetaData.getSettings().getAsLong(LIFECYCLE_ORIGINATION_DATE, -1L);
        }
    }
}
