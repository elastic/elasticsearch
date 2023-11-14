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

import java.util.TreeSet;

/**
 * The Node Metadata version.
 * <p>
 * Prior to 8.8.0, the node {@link Version} was used for almost all kinds of versioning. This class separates the node metadata format
 * version from the running node version. See also {@link NodeMetadata}.
 * <p>
 * Each node metadata version constant has an id number, which for versions prior to 8.12.0 is the same as the release version
 * for backwards compatibility. In 8.12.0 this is changed to an incrementing number, disconnected from the release version.
 * <h2>Version compatibility</h2>
 * The earliest compatible version is hardcoded in the {@link NodeMetadataVersion#MINIMUM_COMPATIBLE} field. This is the earliest version
 * from which we support upgrading Elasticsearch in place.
 * <h2>Adding a new version</h2>
 * A new node metadata version should be added in two cases:
 * <ol>
 *     <li>when a change is made to persisted node metadata and Elasticsearch needs to handle backwards compatibility</li>
 *     <li>when we know that we want a compatibility gate for upgrading in place (for example, 7.17.0 is the minimum compatible version for
 *     8.x releases</li>
 * </ol>
 * Each node metadata version should only be used in a single merged commit (apart from BwC versions copied from {@link Version}).
 * <p>
 * To add a new node metadata version, add a new constant at the bottom of the list that is one greater than the current highest version,
 * and update the {@link #current()} constant to point to the new version.
 * <h2>Reverting a node metadata version</h2>
 * If you revert a commit with a node metadata version change, you <em>must</em> ensure there is a <em>new</em> node metadata version
 * representing the reverted change. <em>Do not</em> let the node metadata version go backwards, it must <em>always</em> be incremented.
 */
public record NodeMetadataVersion(int id) implements VersionId<NodeMetadataVersion> {

    /*
     * NOTE: IntelliJ lies!
     * This map is used during class construction, referenced by the registerIndexVersion method.
     * When all the node metadata version constants have been registered, the map is cleared & never touched again.
     */
    @SuppressWarnings("UnusedAssignment")
    static TreeSet<Integer> IDS = new TreeSet<>();

    private static NodeMetadataVersion def(int id) {
        if (IDS == null) throw new IllegalStateException("The IDS map needs to be present to call this method");

        if (IDS.add(id) == false) {
            throw new IllegalArgumentException("Version id " + id + " defined twice");
        }
        if (id < IDS.last()) {
            throw new IllegalArgumentException("Version id " + id + " is not defined in the right location. Keep constants sorted");
        }
        return new NodeMetadataVersion(id);
    }

    /** Empty value, available for compatibility checks */
    public static final NodeMetadataVersion EMPTY = def(0);
    /** The minimum compatible node metadata version; ES will fail to start for earlier versions */
    public static final NodeMetadataVersion V_7_17_0 = def(7_17_00_99);
    /**
     * The last major version, used in a bootstrap check
     * TODO: can we replace with feature?
     */
    public static final NodeMetadataVersion V_8_0_0 = def(8_00_00_99);
    private static final NodeMetadataVersion CURRENT = def(8_12_00_99);

    static {
        // see comment on IDS field
        // now that we've registered the node metadata versions, we can clear the map
        IDS = null;
    }

    public static final NodeMetadataVersion MINIMUM_COMPATIBLE = V_7_17_0;

    /**
     * Get the current version for on-disk node metadata
     *
     * @return the current version for on-disk node metadata
     */
    public static NodeMetadataVersion current() {
        // not currently pluggable
        return CURRENT;
    }
}
