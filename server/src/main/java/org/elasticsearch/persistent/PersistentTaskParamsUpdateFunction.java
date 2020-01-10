package org.elasticsearch.persistent;

import org.elasticsearch.common.io.stream.NamedWriteable;

import java.util.function.Function;

/**
 * A function to update the params for a persistent task. Notice that this runs on master in the master service thread, so should
 * complete fast.
 * @param <P> the type of the params.
 */
public interface PersistentTaskParamsUpdateFunction<P extends PersistentTaskParams> extends Function<P, P>, NamedWriteable {
}
