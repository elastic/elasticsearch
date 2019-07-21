package graphql.schema.idl;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetcherFactory;
import graphql.schema.TypeResolver;

import java.util.ArrayList;
import java.util.List;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertShouldNeverHappen;

/**
 * This combines a number of {@link WiringFactory}s together to act as one.  It asks each one
 * whether it handles a type and delegates to the first one to answer yes.
 */
public class CombinedWiringFactory implements WiringFactory {
    private final List<WiringFactory> factories;

    public CombinedWiringFactory(List<WiringFactory> factories) {
        assertNotNull(factories, "You must provide a list of wiring factories");
        this.factories = new ArrayList<>(factories);
    }

    @Override
    public boolean providesTypeResolver(InterfaceWiringEnvironment environment) {
        for (WiringFactory factory : factories) {
            if (factory.providesTypeResolver(environment)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public TypeResolver getTypeResolver(InterfaceWiringEnvironment environment) {
        for (WiringFactory factory : factories) {
            if (factory.providesTypeResolver(environment)) {
                return factory.getTypeResolver(environment);
            }
        }
        return assertShouldNeverHappen();
    }

    @Override
    public boolean providesTypeResolver(UnionWiringEnvironment environment) {
        for (WiringFactory factory : factories) {
            if (factory.providesTypeResolver(environment)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public TypeResolver getTypeResolver(UnionWiringEnvironment environment) {
        for (WiringFactory factory : factories) {
            if (factory.providesTypeResolver(environment)) {
                return factory.getTypeResolver(environment);
            }
        }
        return assertShouldNeverHappen();
    }

    @Override
    public boolean providesDataFetcherFactory(FieldWiringEnvironment environment) {
        for (WiringFactory factory : factories) {
            if (factory.providesDataFetcherFactory(environment)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public <T> DataFetcherFactory<T> getDataFetcherFactory(FieldWiringEnvironment environment) {
        for (WiringFactory factory : factories) {
            if (factory.providesDataFetcherFactory(environment)) {
                return factory.getDataFetcherFactory(environment);
            }
        }
        return assertShouldNeverHappen();
    }

    @Override
    public boolean providesDataFetcher(FieldWiringEnvironment environment) {
        for (WiringFactory factory : factories) {
            if (factory.providesDataFetcher(environment)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public DataFetcher getDataFetcher(FieldWiringEnvironment environment) {
        for (WiringFactory factory : factories) {
            if (factory.providesDataFetcher(environment)) {
                return factory.getDataFetcher(environment);
            }
        }
        return assertShouldNeverHappen();
    }
}
