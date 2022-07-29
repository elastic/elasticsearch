/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.SearchPlugin.SearchExtSpec;
import org.elasticsearch.xcontent.ToXContentFragment;

/**
 * Intermediate serializable representation of a search ext section. To be subclassed by plugins that support
 * a custom section as part of a search request, which will be provided within the ext element.
 * Any state needs to be serialized as part of the {@link Writeable#writeTo(StreamOutput)} method and
 * read from the incoming stream, usually done adding a constructor that takes {@link StreamInput} as
 * an argument.
 *
 * Registration happens through {@link SearchPlugin#getSearchExts()}, which also needs a {@link CheckedFunction} that's able to parse
 * the incoming request from the REST layer into the proper {@link SearchExtBuilder} subclass.
 *
 * {@link #getWriteableName()} must return the same name as the one used for the registration
 * of the {@link SearchExtSpec}.
 *
 * @see SearchExtSpec
 */
public abstract class SearchExtBuilder implements VersionedNamedWriteable, ToXContentFragment {

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_EMPTY;
    }
}
