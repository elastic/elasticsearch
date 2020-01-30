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

public abstract class AbstractCleanupCommand extends Command {
    protected final OptionSpec<Long> safetyGapMillisOption;
    protected final OptionSpec<Integer> parallelismOption;
    protected final OptionSpec<String> bucketOption;
    protected final OptionSpec<String> basePathOption;

    public AbstractCleanupCommand(String description) {
        super(description, CommandLoggingConfigurator::configureLoggingWithoutConfig);

        bucketOption = parser.accepts("bucket", "Bucket name").withRequiredArg();

        basePathOption = parser.accepts("base_path", "Base path").withRequiredArg();

        safetyGapMillisOption = parser.accepts("safety_gap_millis", "Safety gap to account for clock drift")
                .withRequiredArg().ofType(Long.class);

        parallelismOption = parser.accepts("parallelism", "How many threads to use to talk to cloud provider")
                .withRequiredArg().ofType(Integer.class);
    }

    // package-private for testing
    OptionParser getParser() {
        return parser;
    }

    // public fo testing
    @Override
    public void execute(Terminal terminal, OptionSet options) throws Exception {
        validate(options);
        AbstractRepository repository = newRepository(terminal, options);

        repository.cleanup();
    }

    protected abstract AbstractRepository newRepository(Terminal terminal, OptionSet options) throws Exception;

    protected void validate(OptionSet options) {
        String bucket = bucketOption.value(options);
        if (Strings.isNullOrEmpty(bucket)) {
            throw new ElasticsearchException("bucket option is required for cleaning up repository");
        }

        String basePath = basePathOption.value(options);
        if (basePath.endsWith("/")) {
            throw new ElasticsearchException("there should be no trailing slash in the base path");
        }

        Long safetyGapMillis = safetyGapMillisOption.value(options);

        if (safetyGapMillis != null && safetyGapMillis < 0L) {
            throw new ElasticsearchException("safety_gap_millis should be non-negative");
        }

        Integer parallelism = parallelismOption.value(options);
        if (parallelism != null && parallelism < 1) {
            throw new ElasticsearchException("parallelism should be at least 1");
        }
    }
}
