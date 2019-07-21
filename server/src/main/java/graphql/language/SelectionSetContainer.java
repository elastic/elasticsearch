package graphql.language;


import graphql.PublicApi;

@PublicApi
public interface SelectionSetContainer<T extends Node> extends Node<T> {
    SelectionSet getSelectionSet();
}
