(function dsbuilder(attr) {
    var urlBuilder = "jdbc:es://";

    if (attr["sslmode"] == "require") {
        urlBuilder += "https://";
    } else{
        urlBuilder += "http://";
    }
    urlBuilder += attr["server"] + ":" + attr["port"];

    var params = [];
    params["user"] = attr["username"];
    params["password"] = attr["password"];

    var formattedParams = [];

    for (var key in params) {
        if (params[key]) {
            var param = encodeURIComponent(params[key]);
            formattedParams.push(connectionHelper.formatKeyValuePair(key, param));
        }
    }

    if (formattedParams.length > 0) {
        urlBuilder += "?" + formattedParams.join("&")
    }

    // logging visible in log.txt if -DLogLevel=Debug is added in Tableau command line
    logging.log("ES JDBC URL before adding additional parameters: " + urlBuilder);

    // TODO: wrap error-prone "free form"
    var additionalConnectionParameters = attr[connectionHelper.attributeWarehouse];
    if (additionalConnectionParameters != null && additionalConnectionParameters.trim().length > 0) {
        urlBuilder += (formattedParams.length == 0) ? "?" : "&";
        urlBuilder += additionalConnectionParameters;
    }

    logging.log("ES JDBC final URL: " + urlBuilder);
    return [urlBuilder];
})
