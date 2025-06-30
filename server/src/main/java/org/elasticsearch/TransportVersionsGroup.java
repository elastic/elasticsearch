/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TransportVersionsGroup {
    public final TransportVersion localTV;
    public final List<TransportVersion> backportTVs;
    public final List<TransportVersion> futureTVs;

    public TransportVersionsGroup(List<TransportVersion> transportVersions) {
        if (transportVersions.stream().sorted().toList().equals(transportVersions) == false) {
            throw new IllegalArgumentException("transportVersions must be sorted by version");
        }

        TransportVersion currentVersion = TransportVersion.current(); // TODO this has to be set somehow

        TransportVersion localTV = null;
        List<TransportVersion> backportTVs = new ArrayList<>();
        List<TransportVersion> futureTVs = new ArrayList<>();

        for (TransportVersion transportVersion : transportVersions) {
            if (transportVersion.onOrAfter(currentVersion) == false) {
                futureTVs.add(transportVersion);
            } else if (localTV == null) {
                localTV = transportVersion;
            } else {
                backportTVs.add(transportVersion);
            }
        }
        if (localTV == null) {
            throw new IllegalArgumentException("TransportVersionsGroup must contain a local TransportVersion");
        }
        this.backportTVs = Collections.unmodifiableList(backportTVs);
        this.futureTVs = Collections.unmodifiableList(futureTVs);
        this.localTV = localTV;
    }

    public boolean isCompatible(TransportVersion version) {
        return version.onOrAfter(localTV)
            || backportTVs.stream().anyMatch(version::isPatchFrom);
    }

    public List<Integer> getLocalAndBackportedTVIds() {
        var localAndBackportedTVs = new ArrayList<TransportVersion>();
        localAndBackportedTVs.add(localTV);
        localAndBackportedTVs.addAll(backportTVs);
        return localAndBackportedTVs.stream().map(TransportVersion::id).toList();
    }
}
