/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import org.elasticsearch.nativeaccess.jna.JnaStaticLinuxCLibrary.JnaStatx;
import org.elasticsearch.nativeaccess.lib.LinuxCLibrary;

class JnaLinuxCLibrary implements LinuxCLibrary {
    @Override
    public statx newStatx() {
        return new JnaStatx();
    }

    @Override
    public int statx(int dirfd, String pathname, int flags, int mask, statx statxbuf) {
        assert statxbuf instanceof JnaStatx;
        var jnaStatxbuf = (JnaStatx) statxbuf;
        return JnaStaticLinuxCLibrary.statx(dirfd, pathname, flags, mask, jnaStatxbuf);
    }
}
