/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import org.elasticsearch.nativeaccess.jna.JnaStaticMacCLibrary.JnaErrorReference;
import org.elasticsearch.nativeaccess.lib.MacCLibrary;

class JnaMacCLibrary implements MacCLibrary {
    @Override
    public ErrorReference newErrorReference() {
        return new JnaErrorReference();
    }

    @Override
    public int sandbox_init(String profile, long flags, ErrorReference errorbuf) {
        assert errorbuf instanceof JnaErrorReference;
        var jnaErrorbuf = (JnaErrorReference) errorbuf;
        return JnaStaticMacCLibrary.sandbox_init(profile, flags, jnaErrorbuf.ref);
    }

    @Override
    public void sandbox_free_error(ErrorReference errorbuf) {
        assert errorbuf instanceof JnaErrorReference;
        var jnaErrorbuf = (JnaErrorReference) errorbuf;
        JnaStaticMacCLibrary.sandbox_free_error(jnaErrorbuf.ref.getValue());
    }
}
