(function propertiesbuilder(attr) {

    var stringProps = [
        ["v-timezone", "timezone"],
        ["v-connectTimeout", "connect.timeout"],
        ["v-networkTimeout", "network.timeout"],
        ["v-pageSize", "page.size"],
        ["v-pageTimeout", "page.timeout"],
        ["v-queryTimeout", "query.timeout"],
        ["v-proxyHttp", "proxy.http"],
        ["v-proxySocks", "proxy.socks"],
        ["v-fieldMultiValueLeniency", "field.multi.value.leniency"],
        ["v-indexIncludeFrozen", "index.include.frozen"],
        ["v-allowPartialSearchResults", "allow.partial.search.results"],
        ["v-validateProperties", "validate.properties"]
    ];

    var props = {};

    props["user"] = attr["username"];
    props["password"] = attr["password"];

    for (var i = 0; i < stringProps.length; i++) {
        var value = attr[stringProps[i][0]];
        if(value != null && value != ""){
            props[stringProps[i][1]] = value;
        }
    }

    props["catalog"] = attr[connectionHelper.attributeDatabase];

    return props;
})
