package co.elastic.elasticsearch.stateless;

import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Encapsulates license checking for the Stateless plugin.
 */
public class StatelessLicenseChecker {

    private final BooleanSupplier supplier;

    /**
     * Constructs a license checker for the Stateless plugin with the specified boolean supplier.
     *
     * @param isAllowed a boolean supplier that should return true if stateless is allowed and otherwise false.
     */
    public StatelessLicenseChecker(final BooleanSupplier isAllowed) {
        this.supplier = Objects.requireNonNull(isAllowed);
    }

    /**
     * @return true if stateless is allowed, otherwise false
     */
    public boolean isStatelessAllowed() {
        return supplier.getAsBoolean();
    }
}
