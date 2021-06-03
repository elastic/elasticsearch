/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_ORIGINATION_DATE;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE;

public class IndexLifecycleOriginationDateParser {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("uuuu.MM.dd");
    private static final String INDEX_NAME_REGEX = "^.*-(\\d{4}.\\d{2}.\\d{2})(-[\\d]+)?$";
    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile(INDEX_NAME_REGEX);

    /**
     * Determines if the origination date needs to be parsed from the index name.
     */
    public static boolean shouldParseIndexName(Settings indexSettings) {
        return indexSettings.getAsLong(LIFECYCLE_ORIGINATION_DATE, -1L) == -1L &&
            indexSettings.getAsBoolean(LIFECYCLE_PARSE_ORIGINATION_DATE, false);
    }

    /**
     * Parses the index according to the supported format and extracts the origination date. If the index does not match the expected
     * format or the date in the index name doesn't match the `yyyy.MM.dd` format it throws an {@link IllegalArgumentException}
     */
    public static long parseIndexNameAndExtractDate(String indexName) {
        Matcher matcher = INDEX_NAME_PATTERN.matcher(indexName);
        if (matcher.matches()) {
            String dateAsString = matcher.group(1);
            try {
                return DATE_FORMATTER.parseMillis(dateAsString);
            } catch (ElasticsearchParseException | IllegalArgumentException e) {
                throw new IllegalArgumentException("index name [" + indexName + "] contains date [" + dateAsString + "] which " +
                    "couldn't be parsed using the 'yyyy.MM.dd' format", e);
            }
        }

        throw new IllegalArgumentException("index name [" + indexName + "] does not match pattern '" + INDEX_NAME_REGEX + "'");
    }
}
