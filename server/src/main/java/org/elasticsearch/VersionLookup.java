/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import java.util.OptionalInt;

public interface VersionLookup {

    /**
     * Returns the id used for {@code version}, or {@link OptionalInt#empty} if that information is not available.
     */
    OptionalInt findId(Version version);

    /**
     * Returns the release version for {@code id}.
     * If there is not an exact release version, a range is returned.
     */
    String inferVersion(int id);
}
