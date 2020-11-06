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
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;

public class CleanupGCSRepositoryCommand extends AbstractCleanupCommand {

    private final OptionSpec<String> base64CredentialsOption;
    private final OptionSpec<String> endpointOption;
    private final OptionSpec<String> tokenURIOption;

    public CleanupGCSRepositoryCommand() {
        super("Command to cleanup orphaned segment files from the GCS repository");

        base64CredentialsOption = parser
                .accepts("base64_credentials", "Base64 encoded content of google service account credentials file")
                .withRequiredArg();
        endpointOption = parser.accepts("endpoint", "GCS endpoint")
                .withRequiredArg();
        tokenURIOption = parser.accepts("token_uri", "GCS URI to use for OAuth tokens")
                .withRequiredArg();
    }

    @Override
    protected void validate(OptionSet options) {
        super.validate(options);

        String encodedCredentials = base64CredentialsOption.value(options);
        if (Strings.isNullOrEmpty(encodedCredentials)) {
            throw new ElasticsearchException("base64_credentials option is required for cleaning up GCS repository");
        }
    }

    @Override
    protected AbstractRepository newRepository(Terminal terminal, OptionSet options)
            throws IOException, GeneralSecurityException, URISyntaxException {
        return new GCSRepository(
                terminal,
                safetyGapMillisOption.value(options),
                parallelismOption.value(options),
                bucketOption.value(options),
                basePathOption.value(options),
                base64CredentialsOption.value(options),
                endpointOption.value(options),
                tokenURIOption.value(options));
    }

}
