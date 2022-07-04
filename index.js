const api = require('binance');

// ===================================
var mysql = require('mysql');


var con = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "",
    database: "cryptowebsocket"
});

con.connect(function(err) {
    if (err) throw err;
    console.log("Connected to the DB !");
});

/*const con = mysql.createPool({
    connectionLimit : 100, // important
    host     : 'localhost',
    user     : 'root',
    password : '',
    database : 'cryptowebsocket',
    debug    :  false
});*/

// ===================================

const binanceWS = new api.BinanceWS(true); // Argument specifies whether the responses should be beautified, defaults to true

/*binanceWS.onDepthUpdate('BNBBTC', (data) => {
    console.log(data);
});*/

/*
binanceWS.onKline('BNBBTC', '1m', (data) => {
    console.log(data);
});
*/


/*binanceWS.onKline('BNBUSDT', '1m', (data) => {
    console.log(data);
});*/

/*binanceWS.onKline('TLMUSDT', '1m', (data) => {
    console.log(data);
});*/


// ================ All Currencies Stream Rates ==================

const streams = binanceWS.streams;

binanceWS.onCombinedStream([
        streams.kline('BNBUSDT', '1m'),
        streams.kline('TLMUSDT', '1m'),
        streams.kline('ETHUSDT', '1m'),
        streams.kline('BTCUSDT', '1m'),
        streams.kline('USDCUSDT', '1m'),
        streams.kline('BUSDUSDT', '1m'),
        streams.kline('ADAUSDT', '1m'),
        streams.kline('SOLUSDT', '1m'),
        streams.kline('DOGEUSDT', '1m'),
        streams.kline('DOTUSDT', '1m'),
        streams.kline('TRXUSDT', '1m'),
        streams.kline('LEOUSDT', '1m'),

        streams.kline('YFIUSDT', '1m'),streams.kline('PAXGUSDT', '1m'),streams.kline('MKRUSDT', '1m'),streams.kline('YFIIUSDT', '1m'),streams.kline('BIFIUSDT', '1m'),streams.kline('AUTOUSDT', '1m'),streams.kline('ILVUSDT', '1m'),streams.kline('BNXUSDT', '1m'),streams.kline('BCHUSDT', '1m'),streams.kline('XMRUSDT', '1m'),streams.kline('KP3RUSDT', '1m'),streams.kline('ZECUSDT', '1m'),streams.kline('AAVEUSDT', '1m'),streams.kline('EGLDUSDT', '1m'),streams.kline('QNTUSDT', '1m'),streams.kline('KSMUSDT', '1m'),streams.kline('LTCUSDT', '1m'),streams.kline('DASHUSDT', '1m'),streams.kline('QUICKUSDT', '1m'),streams.kline('FARMUSDT', '1m'),streams.kline('FTTUSDT', '1m'),streams.kline('BNBUPUSDT', '1m'),streams.kline('DCRUSDT', '1m'),streams.kline('MLNUSDT', '1m'),streams.kline('ALCXUSDT', '1m'),streams.kline('TORNUSDT', '1m'),streams.kline('CREAMUSDT', '1m'),streams.kline('AVAXUSDT', '1m'),streams.kline('BTGUSDT', '1m'),streams.kline('ETCUSDT', '1m'),streams.kline('AXSUSDT', '1m'),streams.kline('ZENUSDT', '1m'),streams.kline('MOVRUSDT', '1m'),streams.kline('TRBUSDT', '1m'),streams.kline('HNTUSDT', '1m'),streams.kline('WNXMUSDT', '1m'),streams.kline('ARUSDT', '1m'),streams.kline('NEOUSDT', '1m'),streams.kline('ENSUSDT', '1m'),streams.kline('NMRUSDT', '1m'),streams.kline('LPTUSDT', '1m'),streams.kline('REPUSDT', '1m'),streams.kline('LINKUSDT', '1m'),streams.kline('ATOMUSDT', '1m'),streams.kline('PSGUSDT', '1m'),streams.kline('ICPUSDT', '1m'),streams.kline('FILUSDT', '1m'),streams.kline('UNFIUSDT', '1m'),streams.kline('BTCSTUSDT', '1m'),streams.kline('WINGUSDT', '1m'),streams.kline('XVSUSDT', '1m'),streams.kline('BTCUPUSDT', '1m'),streams.kline('AUCTIONUSDT', '1m'),streams.kline('WAVESUSDT', '1m'),streams.kline('APEUSDT', '1m'),streams.kline('FXSUSDT', '1m'),streams.kline('BALUSDT', '1m'),streams.kline('CITYUSDT', '1m'),streams.kline('UNIUSDT', '1m'),streams.kline('CVXUSDT', '1m'),streams.kline('PROMUSDT', '1m'),streams.kline('BARUSDT', '1m'),streams.kline('JUVUSDT', '1m'),streams.kline('SANTOSUSDT', '1m'),streams.kline('NEARUSDT', '1m'),streams.kline('PYRUSDT', '1m'),streams.kline('BADGERUSDT', '1m'),streams.kline('MULTIUSDT', '1m'),streams.kline('ATMUSDT', '1m'),streams.kline('CAKEUSDT', '1m'),streams.kline('SNXUSDT', '1m'),streams.kline('ASRUSDT', '1m'),streams.kline('ACMUSDT', '1m'),streams.kline('FORTHUSDT', '1m'),streams.kline('QTUMUSDT', '1m'),streams.kline('OGUSDT', '1m'),streams.kline('GALUSDT', '1m'),streams.kline('BONDUSDT', '1m'),streams.kline('ETHUPUSDT', '1m'),streams.kline('DEXEUSDT', '1m'),streams.kline('UMAUSDT', '1m'),streams.kline('ALPINEUSDT', '1m'),streams.kline('GTCUSDT', '1m'),streams.kline('ALICEUSDT', '1m'),streams.kline('LAZIOUSDT', '1m'),streams.kline('LUNAUSDT', '1m'),streams.kline('OMGUSDT', '1m'),streams.kline('RUNEUSDT', '1m'),streams.kline('PORTOUSDT', '1m'),streams.kline('KDAUSDT', '1m'),streams.kline('KAVAUSDT', '1m'),streams.kline('ANTUSDT', '1m'),streams.kline('RADUSDT', '1m'),streams.kline('MASKUSDT', '1m'),streams.kline('FIROUSDT', '1m'),streams.kline('BANDUSDT', '1m'),streams.kline('XTZUSDT', '1m'),streams.kline('FLOWUSDT', '1m'),streams.kline('API3USDT', '1m'),streams.kline('ERNUSDT', '1m'),streams.kline('DEGOUSDT', '1m'),streams.kline('INJUSDT', '1m'),streams.kline('GHSTUSDT', '1m'),streams.kline('DYDXUSDT', '1m'),streams.kline('KNCUSDT', '1m'),streams.kline('THETAUSDT', '1m'),streams.kline('MTLUSDT', '1m'),streams.kline('GBPUSDT', '1m'),streams.kline('HIGHUSDT', '1m'),streams.kline('ORNUSDT', '1m'),streams.kline('SUSHIUSDT', '1m'),streams.kline('MOBUSDT', '1m'),streams.kline('ADAUPUSDT', '1m'),streams.kline('EURUSDT', '1m'),streams.kline('USDPUSDT', '1m'),streams.kline('TUSDUSDT', '1m'),streams.kline('WBTCUSDT', '1m'),streams.kline('LSKUSDT', '1m'),streams.kline('SCRTUSDT', '1m'),streams.kline('EOSUSDT', '1m'),streams.kline('BETHUSDT', '1m'),streams.kline('CELOUSDT', '1m'),streams.kline('SRMUSDT', '1m'),streams.kline('LITUSDT', '1m'),streams.kline('SANDUSDT', '1m'),streams.kline('TWTUSDT', '1m'),streams.kline('MANAUSDT', '1m'),streams.kline('BELUSDT', '1m'),streams.kline('CTKUSDT', '1m'),streams.kline('XNOUSDT', '1m'),streams.kline('IMXUSDT', '1m'),streams.kline('GMTUSDT', '1m'),streams.kline('MCUSDT', '1m'),streams.kline('RAYUSDT', '1m'),streams.kline('AUDUSDT', '1m'),streams.kline('CRVUSDT', '1m'),streams.kline('NEXOUSDT', '1m'),streams.kline('RLCUSDT', '1m'),

        streams.kline('ADADOWNUSDT', '1m'),
        streams.kline('BNBDOWNUSDT', '1m'),
        streams.kline('BTCDOWNUSDT', '1m'),
        streams.kline('DOTDOWNUSDT', '1m'),
        streams.kline('ETHDOWNUSDT', '1m'),
        streams.kline('LINKDOWNUSDT', '1m'),
        streams.kline('TRXDOWNUSDT', '1m'),
        streams.kline('XRPDOWNUSDT', '1m'),

    ],
    (streamEvent) => {
    switch(streamEvent.stream) {
            case streams.kline('BNBUSDT', '1m'):
            case streams.kline('TLMUSDT', '1m'):
            case streams.kline('ETHUSDT', '1m'):
            case streams.kline('BTCUSDT', '1m'):
            case streams.kline('USDCUSDT', '1m'):
            case streams.kline('BUSDUSDT', '1m'):
            case streams.kline('ADAUSDT', '1m'):
            case streams.kline('SOLUSDT', '1m'):
            case streams.kline('DOGEUSDT', '1m'):
            case streams.kline('DOTUSDT', '1m'):
            case streams.kline('TRXUSDT', '1m'):
            case streams.kline('LEOUSDT', '1m'):

            case streams.kline('YFIUSDT', '1m'):
            case streams.kline('PAXGUSDT', '1m'):
            case streams.kline('MKRUSDT', '1m'):
            case streams.kline('YFIIUSDT', '1m'):
            case streams.kline('BIFIUSDT', '1m'):
            case streams.kline('AUTOUSDT', '1m'):
            case streams.kline('ILVUSDT', '1m'):
            case streams.kline('BNXUSDT', '1m'):
            case streams.kline('BCHUSDT', '1m'):
            case streams.kline('XMRUSDT', '1m'):
            case streams.kline('KP3RUSDT', '1m'):
            case streams.kline('ZECUSDT', '1m'):
            case streams.kline('AAVEUSDT', '1m'):
            case streams.kline('EGLDUSDT', '1m'):
            case streams.kline('QNTUSDT', '1m'):
            case streams.kline('KSMUSDT', '1m'):
            case streams.kline('LTCUSDT', '1m'):
            case streams.kline('DASHUSDT', '1m'):
            case streams.kline('QUICKUSDT', '1m'):
            case streams.kline('FARMUSDT', '1m'):
            case streams.kline('FTTUSDT', '1m'):
            case streams.kline('BNBUPUSDT', '1m'):
            case streams.kline('DCRUSDT', '1m'):
            case streams.kline('MLNUSDT', '1m'):
            case streams.kline('ALCXUSDT', '1m'):
            case streams.kline('TORNUSDT', '1m'):
            case streams.kline('CREAMUSDT', '1m'):
            case streams.kline('AVAXUSDT', '1m'):
            case streams.kline('BTGUSDT', '1m'):
            case streams.kline('ETCUSDT', '1m'):
            case streams.kline('AXSUSDT', '1m'):
            case streams.kline('ZENUSDT', '1m'):
            case streams.kline('MOVRUSDT', '1m'):
            case streams.kline('TRBUSDT', '1m'):
            case streams.kline('HNTUSDT', '1m'):
            case streams.kline('WNXMUSDT', '1m'):
            case streams.kline('ARUSDT', '1m'):
            case streams.kline('NEOUSDT', '1m'):
            case streams.kline('ENSUSDT', '1m'):
            case streams.kline('NMRUSDT', '1m'):
            case streams.kline('LPTUSDT', '1m'):
            case streams.kline('REPUSDT', '1m'):
            case streams.kline('LINKUSDT', '1m'):
            case streams.kline('ATOMUSDT', '1m'):
            case streams.kline('PSGUSDT', '1m'):
            case streams.kline('ICPUSDT', '1m'):
            case streams.kline('FILUSDT', '1m'):
            case streams.kline('UNFIUSDT', '1m'):
            case streams.kline('BTCSTUSDT', '1m'):
            case streams.kline('WINGUSDT', '1m'):
            case streams.kline('XVSUSDT', '1m'):
            case streams.kline('BTCUPUSDT', '1m'):
            case streams.kline('AUCTIONUSDT', '1m'):
            case streams.kline('WAVESUSDT', '1m'):
            case streams.kline('APEUSDT', '1m'):
            case streams.kline('FXSUSDT', '1m'):
            case streams.kline('BALUSDT', '1m'):
            case streams.kline('CITYUSDT', '1m'):
            case streams.kline('UNIUSDT', '1m'):
            case streams.kline('CVXUSDT', '1m'):
            case streams.kline('PROMUSDT', '1m'):
            case streams.kline('BARUSDT', '1m'):
            case streams.kline('JUVUSDT', '1m'):
            case streams.kline('SANTOSUSDT', '1m'):
            case streams.kline('NEARUSDT', '1m'):
            case streams.kline('PYRUSDT', '1m'):
            case streams.kline('BADGERUSDT', '1m'):
            case streams.kline('MULTIUSDT', '1m'):
            case streams.kline('ATMUSDT', '1m'):
            case streams.kline('CAKEUSDT', '1m'):
            case streams.kline('SNXUSDT', '1m'):
            case streams.kline('ASRUSDT', '1m'):
            case streams.kline('ACMUSDT', '1m'):
            case streams.kline('FORTHUSDT', '1m'):
            case streams.kline('QTUMUSDT', '1m'):
            case streams.kline('OGUSDT', '1m'):
            case streams.kline('GALUSDT', '1m'):
            case streams.kline('BONDUSDT', '1m'):
            case streams.kline('ETHUPUSDT', '1m'):
            case streams.kline('DEXEUSDT', '1m'):
            case streams.kline('UMAUSDT', '1m'):
            case streams.kline('ALPINEUSDT', '1m'):
            case streams.kline('GTCUSDT', '1m'):
            case streams.kline('ALICEUSDT', '1m'):
            case streams.kline('LAZIOUSDT', '1m'):
            case streams.kline('LUNAUSDT', '1m'):
            case streams.kline('OMGUSDT', '1m'):
            case streams.kline('RUNEUSDT', '1m'):
            case streams.kline('PORTOUSDT', '1m'):
            case streams.kline('KDAUSDT', '1m'):
            case streams.kline('KAVAUSDT', '1m'):
            case streams.kline('ANTUSDT', '1m'):
            case streams.kline('RADUSDT', '1m'):
            case streams.kline('MASKUSDT', '1m'):
            case streams.kline('FIROUSDT', '1m'):
            case streams.kline('BANDUSDT', '1m'):
            case streams.kline('XTZUSDT', '1m'):
            case streams.kline('FLOWUSDT', '1m'):
            case streams.kline('API3USDT', '1m'):
            case streams.kline('ERNUSDT', '1m'):
            case streams.kline('DEGOUSDT', '1m'):
            case streams.kline('INJUSDT', '1m'):
            case streams.kline('GHSTUSDT', '1m'):
            case streams.kline('DYDXUSDT', '1m'):
            case streams.kline('KNCUSDT', '1m'):
            case streams.kline('THETAUSDT', '1m'):
            case streams.kline('MTLUSDT', '1m'):
            case streams.kline('GBPUSDT', '1m'):
            case streams.kline('HIGHUSDT', '1m'):
            case streams.kline('ORNUSDT', '1m'):
            case streams.kline('SUSHIUSDT', '1m'):
            case streams.kline('MOBUSDT', '1m'):
            case streams.kline('ADAUPUSDT', '1m'):
            case streams.kline('EURUSDT', '1m'):
            case streams.kline('USDPUSDT', '1m'):
            case streams.kline('TUSDUSDT', '1m'):
            case streams.kline('WBTCUSDT', '1m'):
            case streams.kline('LSKUSDT', '1m'):
            case streams.kline('SCRTUSDT', '1m'):
            case streams.kline('EOSUSDT', '1m'):
            case streams.kline('BETHUSDT', '1m'):
            case streams.kline('CELOUSDT', '1m'):
            case streams.kline('SRMUSDT', '1m'):
            case streams.kline('LITUSDT', '1m'):
            case streams.kline('SANDUSDT', '1m'):
            case streams.kline('TWTUSDT', '1m'):
            case streams.kline('MANAUSDT', '1m'):
            case streams.kline('BELUSDT', '1m'):
            case streams.kline('CTKUSDT', '1m'):
            case streams.kline('XNOUSDT', '1m'):
            case streams.kline('IMXUSDT', '1m'):
            case streams.kline('GMTUSDT', '1m'):
            case streams.kline('MCUSDT', '1m'):
            case streams.kline('RAYUSDT', '1m'):
            case streams.kline('AUDUSDT', '1m'):
            case streams.kline('CRVUSDT', '1m'):
            case streams.kline('NEXOUSDT', '1m'):
            case streams.kline('RLCUSDT', '1m'):

            case streams.kline('ADADOWNUSDT', '1m'):
            case streams.kline('BNBDOWNUSDT', '1m'):
            case streams.kline('BTCDOWNUSDT', '1m'):
            case streams.kline('DOTDOWNUSDT', '1m'):
            case streams.kline('ETHDOWNUSDT', '1m'):
            case streams.kline('LINKDOWNUSDT', '1m'):
            case streams.kline('TRXDOWNUSDT', '1m'):
            case streams.kline('XRPDOWNUSDT', '1m'):


            //console.log('Kline event, update 1m Currency (BNBUSDT) \n', streamEvent.data);

                var jsonData = JSON.parse(JSON.stringify(streamEvent.data));
                var klineData = jsonData.kline;


                /* ================== This is to be used with connection pool =================== */
                /*con.getConnection((err, connection) => {
                    //if(err) throw err;

                        var sqlEx = "SELECT COUNT(id) AS exists_counter " +
                            " FROM crypto_feed " +
                            " WHERE symbol = '"+ klineData.symbol +"' " +
                            " AND startTime = '"+ klineData.startTime +"' " +
                            " AND endTime = '"+ klineData.endTime +"' " +
                            " AND open = '"+ klineData.open +"' " +
                            " AND close = '"+ klineData.close +"' ";
                        connection.query(sqlEx, function (err, rows) {
                            //console.log(rows.length +'------'+ klineData.symbol +'-'+ klineData.startTime + '-' + klineData.endTime + '-' +klineData.open + '-' +klineData.close);
                            if (rows.length) {
                                    if(rows[0].exists_counter == 0) {
                                        var sql = "INSERT INTO crypto_feed (startTime, endTime, start_date, end_date, symbol, rate_interval, firstTradeId, lastTradeId, open, close, high, low, volume, trades, final, quoteVolume, volumeActive, quoteVolumeActive, ignored) VALUES ('"+ klineData.startTime +"', '"+ klineData.endTime +"', '"+ unixtime_to_datetime(klineData.startTime) +"', '"+ unixtime_to_datetime(klineData.endTime) +"', '"+ klineData.symbol +"', '"+ klineData.interval +"', "+ klineData.firstTradeId +", "+klineData.lastTradeId+", "+klineData.open+", "+klineData.close+", "+klineData.high+", "+klineData.low+", "+klineData.volume+", "+klineData.trades+", '"+klineData.final+"', "+klineData.quoteVolume+", "+klineData.volumeActive+", "+klineData.quoteVolumeActive+", '"+klineData.ignored+"')";
                                        connection.query(sql, (err, result) => {
                                            connection.release(); // return the connection to pool
                                            //if(err) throw err;
                                            //console.log("1 record inserted");
                                        });
                                    }
                        }
                    });

                });*/
                /* ================== This is to be used with connection pool =================== */



                /* ================== This is to be used with simple MYSQL connection =================== */
                    var sqlEx = "SELECT COUNT(id) AS exists_counter " +
                        " FROM crypto_feed " +
                        " WHERE symbol = '"+ klineData.symbol +"' " +
                        " AND startTime = '"+ klineData.startTime +"' " +
                        " AND endTime = '"+ klineData.endTime +"' " +
                        " AND open = '"+ klineData.open +"' " +
                        " AND close = '"+ klineData.close +"' ";
                    con.query(sqlEx, function (err, rows) {
                        // console.log(rows.length +'-'+ rows[0].exists_counter + '-' + klineData.symbol +'-'+ klineData.startTime + '-' + klineData.endTime + '-' +klineData.open + '-' +klineData.close);
                        if (rows.length) {
                            if(rows[0].exists_counter == 0) {
                                var sql = "INSERT INTO crypto_feed (startTime, endTime, start_date, end_date, symbol, rate_interval, firstTradeId, lastTradeId, open, close, high, low, volume, trades, final, quoteVolume, volumeActive, quoteVolumeActive, ignored) VALUES ('"+ klineData.startTime +"', '"+ klineData.endTime +"', '"+ unixtime_to_datetime(klineData.startTime) +"', '"+ unixtime_to_datetime(klineData.endTime) +"', '"+ klineData.symbol +"', '"+ klineData.interval +"', "+ klineData.firstTradeId +", "+klineData.lastTradeId+", "+klineData.open+", "+klineData.close+", "+klineData.high+", "+klineData.low+", "+klineData.volume+", "+klineData.trades+", '"+klineData.final+"', "+klineData.quoteVolume+", "+klineData.volumeActive+", "+klineData.quoteVolumeActive+", '"+klineData.ignored+"')";
                                con.query(sql, (err, result) => {
                                //if(err) throw err;
                                //console.log("1 record inserted");
                            });

                            //========== Insert data in extended Table ==========
                                var sql_ext = "INSERT INTO crypto_feed_extended (startTime, endTime, start_date, end_date, symbol, rate_interval, firstTradeId, lastTradeId, open, close, high, low, volume, trades, final, quoteVolume, volumeActive, quoteVolumeActive, ignored) VALUES ('"+ klineData.startTime +"', '"+ klineData.endTime +"', '"+ unixtime_to_datetime(klineData.startTime) +"', '"+ unixtime_to_datetime(klineData.endTime) +"', '"+ klineData.symbol +"', '"+ klineData.interval +"', "+ klineData.firstTradeId +", "+klineData.lastTradeId+", "+klineData.open+", "+klineData.close+", "+klineData.high+", "+klineData.low+", "+klineData.volume+", "+klineData.trades+", '"+klineData.final+"', "+klineData.quoteVolume+", "+klineData.volumeActive+", "+klineData.quoteVolumeActive+", '"+klineData.ignored+"')";
                                con.query(sql_ext, (err, result) => {
                                    //if(err) throw err;
                                    //console.log("1 record inserted");
                                });
                            //========== Insert data in extended Table ==========

                            }
                        }
                    });
                /* ================== This is to be used with simple MYSQL connection =================== */



                break;
}
}
);

    //============= Remove the older data that is 60-minutes before interval
    function remove_older_data_from_master_data() {

        /* ================== This is to be used with connection pool =================== */
        /*con.getConnection((err, connection) => {
            if(err) throw err;

                var sql = "DELETE FROM crypto_feed WHERE start_date < NOW() - INTERVAL 60 MINUTE";
                connection.query(sql, (err, result) => {
                    connection.release(); // return the connection to pool
                if(err) throw err;


                console.log("Number of records deleted: " + result.affectedRows);
            });
        });*/
        /* ================== This is to be used with connection pool =================== */


        /* ================== This is to be used with simple MYSQL connection =================== */
        var sql = "DELETE FROM crypto_feed WHERE start_date < NOW() - INTERVAL 60 MINUTE";
        con.query(sql, (err, result) => {
                if(err) throw err;

                //console.log("Number of records deleted: " + result.affectedRows);
        });
        /* ================== This is to be used with simple MYSQL connection =================== */

    }
    setInterval(remove_older_data_from_master_data, (1000 * 60 * 60) );
    //============= Remove the older data that is 60-minutes before interval


        //============= Remove the older data from extended table that is 200-minutes before interval
        function remove_older_data_from_extended_table() {
            var sql = "DELETE FROM crypto_feed_extended WHERE start_date < NOW() - INTERVAL 200 MINUTE";
            con.query(sql, (err, result) => {
                if(err) throw err;
                    //console.log("Number of records deleted: " + result.affectedRows);
                });
        }
        setInterval(remove_older_data_from_extended_table, (1000 * 60 * 200) );
        //============= Remove the older data from extended table that is 200-minutes before interval


    function unixtime_to_datetime(timestamp) {
        var datetime;
        date = new Date(timestamp);
        var year = date.getFullYear();
        var month = date.getMonth() + 1;
        var day = date.getDate();
        var hours = date.getHours();
        var minutes = date.getMinutes();
        var seconds = date.getSeconds();

        datetime = year + "-" + month + "-" + day + " " + hours + ":" + minutes + ":" + seconds;
        return datetime;
    }