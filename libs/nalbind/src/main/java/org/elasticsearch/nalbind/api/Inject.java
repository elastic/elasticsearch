package org.elasticsearch.nalbind.api;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Designates a constructor to be called by the injector.
 */
@Target(CONSTRUCTOR)
@Retention(RUNTIME)
public @interface Inject {
}
