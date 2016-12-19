/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
        e = expectThrows(IllegalArgumentException.class,
                () -> new PutModelSnapshotDescriptionAction.Request("foo", null, "bar"));
        assertEquals("[snapshot_id] must not be null.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class,
                () -> new PutModelSnapshotDescriptionAction.Request("foo", "foo", null));
        assertEquals("[description] must not be null.", e.getMessage());
    }


}
