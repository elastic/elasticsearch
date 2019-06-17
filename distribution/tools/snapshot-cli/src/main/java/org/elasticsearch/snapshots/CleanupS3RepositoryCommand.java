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

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;

public class CleanupS3RepositoryCommand extends EnvironmentAwareCommand {

    private final OptionSpec<String> regionOption;
    private final OptionSpec<String> bucketOption;
    private final OptionSpec<String> accessKeyOption;
    private final OptionSpec<String> secretKeyOption;

    public CleanupS3RepositoryCommand() {
        super("Command to cleanup leaked segment files from the S3 repository");

        regionOption = parser.accepts("region", "S3 region")
                .withRequiredArg();

        bucketOption = parser.accepts("bucketOption", "Bucket name")
                .withRequiredArg();

        accessKeyOption = parser.accepts("access_key", "Access key")
                .withRequiredArg();

        secretKeyOption = parser.accepts("secret_key", "Secret key")
                .withRequiredArg();
    }


    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        terminal.println("Cleanup tool is running");

        String region = regionOption.value(options);
        if (region == null) {
            terminal.println("region option is required for cleaning up S3 repository");
            return;
        }

        String bucket = bucketOption.value(options);
        if (bucket == null) {
            terminal.println("bucket option is required for cleaning up S3 repository");
            return;
        }

        String accessKey = accessKeyOption.value(options);
        if (accessKey == null) {
            terminal.println("access_key option is required for cleaning up S3 repository");
            return;
        }

        String secretKey = secretKeyOption.value(options);
        if (secretKey == null) {
            terminal.println("secret_key option is required for cleaning up S3 repository");
            return;
        }

        Repository repository = new S3Repository(terminal, region, accessKey, secretKey, bucket);
        repository.cleanup();
    }


}
