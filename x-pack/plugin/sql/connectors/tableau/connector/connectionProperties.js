(function propertiesbuilder(attr) {
    var props = {};

    props["user"] = attr["username"];
    props["password"] = attr["password"];

    var extraProps = attr[connectionHelper.attributeWarehouse];
    if (extraProps != null && extraProps.trim().length > 0) {
        // allow `&` and white-space as attribue-value pair delimiters
        var avps = extraProps.trim().split(/[\s&]/);
        for (var i = 0; i < avps.length; i++) {
            var tokens = avps[i].split("=");
            if (tokens.length != 2 || tokens[0].length == 0 || tokens[1].length == 0) {
                var errMessage = "Invalid additional settings property `" + avps[i] + "`: " +
                    "not conforming to the attribute=value format."
                return connectionHelper.ThrowTableauException(errMessage);
            } else {
                props[tokens[0]] = tokens[1];
            }
        }
    }

    return props;
})
