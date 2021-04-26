/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

<<<<<<< HEAD:server/src/main/java/org/elasticsearch/discovery/zen/PingContextProvider.java
package org.elasticsearch.discovery.zen;
=======
package org.elasticsearch.gradle.internal.test.rest.transform.text;
>>>>>>> 5bcd02cb4d2... Restructure build tools java packages (#72030):buildSrc/src/main/java/org/elasticsearch/gradle/internal/test/rest/transform/text/ReplaceIsTrue.java

import org.elasticsearch.cluster.ClusterState;

public interface PingContextProvider {

    /** return the current cluster state of the node */
    ClusterState clusterState();
}
