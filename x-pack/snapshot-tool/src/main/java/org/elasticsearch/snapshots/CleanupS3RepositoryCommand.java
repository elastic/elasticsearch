/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.Strings;

public class CleanupS3RepositoryCommand extends AbstractCleanupCommand {

    private final OptionSpec<String> regionOption;
    private final OptionSpec<String> endpointOption;
    private final OptionSpec<String> accessKeyOption;
    private final OptionSpec<String> secretKeyOption;

    public CleanupS3RepositoryCommand() {
        super("Command to cleanup orphaned segment files from the S3 repository");

        regionOption = parser.accepts("region", "S3 region")
                .withRequiredArg();

        endpointOption = parser.accepts("endpoint", "S3 endpoint")
                .withRequiredArg();

        accessKeyOption = parser.accepts("access_key", "Access key")
                .withRequiredArg();

        secretKeyOption = parser.accepts("secret_key", "Secret key")
                .withRequiredArg();
    }

    @Override
    protected AbstractRepository newRepository(Terminal terminal, OptionSet options) {
        return new S3Repository(
                terminal,
                safetyGapMillisOption.value(options),
                parallelismOption.value(options),
                bucketOption.value(options),
                basePathOption.value(options),
                accessKeyOption.value(options),
                secretKeyOption.value(options),
                endpointOption.value(options),
                regionOption.value(options));
    }

    @Override
    protected void validate(OptionSet options) {
        super.validate(options);

        String region = regionOption.value(options);
        String endpoint = endpointOption.value(options);

        if (Strings.isNullOrEmpty(region) && Strings.isNullOrEmpty(endpoint)) {
            throw new ElasticsearchException("region or endpoint option is required for cleaning up S3 repository");
        }

        if (Strings.isNullOrEmpty(region) == false && Strings.isNullOrEmpty(endpoint) == false) {
            throw new ElasticsearchException("you must not specify both region and endpoint");
        }

        String accessKey = accessKeyOption.value(options);
        if (Strings.isNullOrEmpty(accessKey)) {
            throw new ElasticsearchException("access_key option is required for cleaning up S3 repository");
        }

        String secretKey = secretKeyOption.value(options);
        if (Strings.isNullOrEmpty(secretKey)) {
            throw new ElasticsearchException("secret_key option is required for cleaning up S3 repository");
        }
    }

}
