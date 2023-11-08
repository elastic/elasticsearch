/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.env;

import org.elasticsearch.Version;
import org.elasticsearch.common.VersionId;

/**
 * The index version.
 * <p>
 * Prior to 8.8.0, the node {@link Version} was used for almost all kinds of versioning. This class separates the node metadata format
 * version from the running node version.
 * <p>
 * Each node metadata version constant has an id number, which for versions prior to 8.12.0 is the same as the release version
 * for backwards compatibility. In 8.12.0 this is changed to an incrementing number, disconnected from the release version.
 * <h2>Version compatibility</h2>
 * The earliest compatible version is hardcoded in the {@link NodeMetadataVersion#MINIMUM_COMPATIBLE} field. Previously, this was
 * dynamically calculated from the major/minor versions of {@link Version}, but {@link NodeMetadataVersion} does not have separate
 * major/minor version numbers. So the minimum compatible version is hard-coded as the index version used by the first version of the
 * previous major release.
 * {@link NodeMetadataVersion#MINIMUM_COMPATIBLE} should be updated appropriately whenever a major release happens.
 * <h2>Adding a new version</h2>
 * A new node metadata version should be added in two cases:
 * <ol>
 *     <li>when a change is made to persisted node metadata and Elasticsearch needs to handle backwards compatibility</li>
 *     <li>when we know that we want a compatibility gate for upgrading in place (e.g. 7.17.0 is the minimum compatible version for
 *     8.x releases</li>
 * </ol>
 * Each node metadata version should only be used in a single merged commit (apart from BwC versions copied from {@link Version}).
 * <p>
 * To add a new node metadata version, add a new constant at the bottom of the list that is one greater than the current highest version,
 * and update the {@link #current()} constant to point to the new version.
 * <h2>Reverting an index version</h2>
 * If you revert a commit with a node metadata version change, you <em>must</em> ensure there is a <em>new</em> node metadata version
 * representing the reverted change. <em>Do not</em> let the node metadata version go backwards, it must <em>always</em> be incremented.
 */
public record NodeMetadataVersion(int id) implements VersionId<NodeMetadataVersion> {
    private static final NodeMetadataVersion CURRENT = new NodeMetadataVersion(8_12_00_99);
    public static final NodeMetadataVersion EMPTY = new NodeMetadataVersion(0);
    public static final NodeMetadataVersion MINIMUM_COMPATIBLE = new NodeMetadataVersion(7_17_00_99);
    public static final NodeMetadataVersion V_8_0_0 = new NodeMetadataVersion(8_00_00_99);

    public static NodeMetadataVersion current() {
        return CURRENT;
    }
}
