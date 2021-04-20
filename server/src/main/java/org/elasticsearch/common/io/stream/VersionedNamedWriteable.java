/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.Version;

/**
 * A {@link NamedWriteable} that has a minimum version associated with it.
 */
public interface VersionedNamedWriteable extends NamedWriteable {

    /**
     * Returns the name of the writeable object
     */
    String getWriteableName();

    /**
     * The minimal version of the recipient this object can be sent to
     */
    Version getMinimalSupportedVersion();
}
