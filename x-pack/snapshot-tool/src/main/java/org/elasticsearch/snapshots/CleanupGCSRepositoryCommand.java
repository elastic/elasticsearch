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

public class CleanupGCSRepositoryCommand extends Command {

    private final OptionSpec<String> credentialsFileOption;
    private final OptionSpec<String> bucketOption;
    private final OptionSpec<String> basePathOption;
    private final OptionSpec<Long> safetyGapMillisOption;
    private final OptionSpec<Integer> parallelismOption;

    public CleanupGCSRepositoryCommand() {
        super("Command to cleanup orphaned segment files from the GCS repository",
            CommandLoggingConfigurator::configureLoggingWithoutConfig);

        credentialsFileOption = parser.accepts("credentialsFile", "Name of the file with GCS credentials")
                .withRequiredArg();

        bucketOption = parser.accepts("bucket", "Bucket name")
                .withRequiredArg();

        basePathOption = parser.accepts("base_path", "Base path")
                .withRequiredArg();

        safetyGapMillisOption = parser.accepts("safety_gap_millis", "Safety gap to account for clock drift")
                .withRequiredArg().ofType(Long.class);

        parallelismOption = parser.accepts("parallelism", "How many threads to use to talk to S3")
                .withRequiredArg().ofType(Integer.class);
    }


    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        String credentialsFile = credentialsFileOption.value(options);
        if (Strings.isNullOrEmpty(credentialsFile)) {
            throw new ElasticsearchException("credentialsFile option is required for cleaning up GCS repository");
        }

        String bucket = bucketOption.value(options);
        if (Strings.isNullOrEmpty(bucket)) {
            throw new ElasticsearchException("bucket option is required for cleaning up GCS repository");
        }

        String basePath = basePathOption.value(options);
        if (basePath.endsWith("/")) {
            throw new ElasticsearchException("there should be not trailing slash in the base path");
        }

        Long safetyGapMillis = safetyGapMillisOption.value(options);

        if (safetyGapMillis != null && safetyGapMillis < 0L) {
            throw new ElasticsearchException("safety_gap_millis should be non-negative");
        }

        Integer parallelism = parallelismOption.value(options);
        if (parallelism != null && parallelism < 1) {
            throw new ElasticsearchException("parallelism should be at least 1");
        }

        Repository repository = new GCSRepository(terminal, safetyGapMillis, parallelism, credentialsFile, bucket, basePath);
        repository.cleanup();
    }

    // package-private for testing
    OptionParser getParser() {
        return parser;
    }

}
