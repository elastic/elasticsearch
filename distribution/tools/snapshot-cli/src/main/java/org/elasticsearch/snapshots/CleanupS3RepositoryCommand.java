/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.snapshots;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.Strings;
import org.elasticsearch.env.Environment;

public class CleanupS3RepositoryCommand extends EnvironmentAwareCommand {

    private final OptionSpec<String> regionOption;
    private final OptionSpec<String> endpointOption;
    private final OptionSpec<String> bucketOption;
    private final OptionSpec<String> basePathOption;
    private final OptionSpec<String> accessKeyOption;
    private final OptionSpec<String> secretKeyOption;
    private final OptionSpec<Long> safetyGapMillisOption;
    private final OptionSpec<Integer> parallelismOption;

    public CleanupS3RepositoryCommand() {
        super("Command to cleanup orphaned segment files from the S3 repository");

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
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        terminal.println("Cleanup tool is running");

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
