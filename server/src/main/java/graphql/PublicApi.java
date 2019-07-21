package graphql;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;

/**
 * This represents code that the graphql-java project considers public API and has an imperative to be stable within
 * major releases.
 *
 * The guarantee  is for code calling classes and interfaces with this annotation, not derived from them.  New methods
 * maybe be added which would break derivations but not callers.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(value = {CONSTRUCTOR, METHOD, TYPE})
@Documented
public @interface PublicApi {
}
