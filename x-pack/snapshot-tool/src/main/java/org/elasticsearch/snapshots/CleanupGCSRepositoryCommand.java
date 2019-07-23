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

import java.io.IOException;
import java.security.GeneralSecurityException;

public class CleanupGCSRepositoryCommand extends AbstractCleanupCommand {

    private final OptionSpec<String> credentialsFileOption;

    public CleanupGCSRepositoryCommand() {
        super("Command to cleanup orphaned segment files from the GCS repository");

        credentialsFileOption = parser.accepts("credentials_file", "Name of the file with GCS credentials")
                .withRequiredArg();
    }

    @Override
    protected void validate(OptionSet options) {
        super.validate(options);

        String credentialsFile = credentialsFileOption.value(options);
        if (Strings.isNullOrEmpty(credentialsFile)) {
            throw new ElasticsearchException("credentials_file option is required for cleaning up GCS repository");
        }
    }

    @Override
    protected AbstractRepository newRepository(Terminal terminal, OptionSet options) throws IOException, GeneralSecurityException {
        return new GCSRepository(
                terminal,
                safetyGapMillisOption.value(options),
                parallelismOption.value(options),
                bucketOption.value(options),
                basePathOption.value(options),
                credentialsFileOption.value(options));
    }

}
