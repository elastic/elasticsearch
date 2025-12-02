/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.cli;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

interface PgpSignatureVerifier {

    /**
     * Verify the signature of a zip file using the provided signature input stream.
     * @see BcPgpSignatureVerifier
     * @see IsolatedBcPgpSignatureVerifier
     */
    void verifySignature(Path libDir, Path zip, String urlString, InputStream ascInputStream) throws IOException;

}
