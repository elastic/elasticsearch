/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandLoggingConfigurator;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.Strings;

public class CleanupS3RepositoryCommand extends Command {

    private final OptionSpec<String> regionOption;
    private final OptionSpec<String> endpointOption;
    private final OptionSpec<String> bucketOption;
    private final OptionSpec<String> basePathOption;
    private final OptionSpec<String> accessKeyOption;
    private final OptionSpec<String> secretKeyOption;
    private final OptionSpec<Long> safetyGapMillisOption;
    private final OptionSpec<Integer> parallelismOption;

    public CleanupS3RepositoryCommand() {
        super("Command to cleanup orphaned segment files from the S3 repository",
            CommandLoggingConfigurator::configureLoggingWithoutConfig);

        regionOption = parser.accepts("region", "S3 region")
                .withRequiredArg();

        endpointOption = parser.accepts("endpoint", "S3 endpoint")
                .withRequiredArg();

        bucketOption = parser.accepts("bucket", "Bucket name")
                .withRequiredArg();

        basePathOption = parser.accepts("base_path", "Base path")
                .withRequiredArg();

        accessKeyOption = parser.accepts("access_key", "Access key")
                .withRequiredArg();

        secretKeyOption = parser.accepts("secret_key", "Secret key")
                .withRequiredArg();

        safetyGapMillisOption = parser.accepts("safety_gap_millis", "Safety gap to account for clock drift")
                .withRequiredArg().ofType(Long.class);

        parallelismOption = parser.accepts("parallelism", "How many threads to use to talk to S3")
                .withRequiredArg().ofType(Integer.class);
    }


    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        String region = regionOption.value(options);
        String endpoint = endpointOption.value(options);

        if (Strings.isNullOrEmpty(region) && Strings.isNullOrEmpty(endpoint)) {
            throw new ElasticsearchException("region or endpoint option is required for cleaning up S3 repository");
        }

        if (Strings.isNullOrEmpty(region) == false && Strings.isNullOrEmpty(endpoint) == false) {
            throw new ElasticsearchException("you must not specify both region and endpoint");
        }

        String bucket = bucketOption.value(options);
        if (Strings.isNullOrEmpty(bucket)) {
            throw new ElasticsearchException("bucket option is required for cleaning up S3 repository");
        }

        String basePath = basePathOption.value(options);
        if (basePath.endsWith("/")) {
            throw new ElasticsearchException("there should be not trailing slash in the base path");
        }

        String accessKey = accessKeyOption.value(options);
        if (Strings.isNullOrEmpty(accessKey)) {
            throw new ElasticsearchException("access_key option is required for cleaning up S3 repository");
        }

        String secretKey = secretKeyOption.value(options);
        if (Strings.isNullOrEmpty(secretKey)) {
            throw new ElasticsearchException("secret_key option is required for cleaning up S3 repository");
        }

        Long safetyGapMillis = safetyGapMillisOption.value(options);

        if (safetyGapMillis != null && safetyGapMillis < 0L) {
            throw new ElasticsearchException("safety_gap_millis should be non-negative");
        }

        Integer parallelism = parallelismOption.value(options);
        if (parallelism != null && parallelism < 1) {
            throw new ElasticsearchException("parallelism should be at least 1");
        }

        Repository repository = new S3Repository(terminal, safetyGapMillis, parallelism, endpoint, region, accessKey, secretKey, bucket,
                basePath);
        repository.cleanup();
    }

    // package-private for testing
    OptionParser getParser() {
        return parser;
    }


}
