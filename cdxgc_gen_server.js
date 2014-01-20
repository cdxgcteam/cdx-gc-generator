#!/usr/bin/env node
// ----------------------------
// Info:
// ----------------------------
// Title: cdxgc_gen_server.js
// Description: CDX Grey Cell Tasking Generator Server
// Author: Derek Yap <zangzi@gmail.com>
// License: MIT
// Version: 0.0.1
var version = "0.0.1";

// ----------------------------
// Requires:
// ----------------------------
// - Built-ins:
var util = require('util');
var fs = require('fs');
var crypto = require('crypto');

// - Underscore
var _ = require('underscore');

// - Random number generators:
var randgen = require('random-seed');
var mainrand = null;
var wellprng = require('well-rng');
var well = new wellprng();

// - AMQP -> RabbitMQ Connection Library
var amqp = require('amqplib');
// - CSV
var csvparser = require('csv');
// - Promises...
var when = require('when');
//var q = require('q');
// - Logging
var winston = require('winston');
var logger = new (winston.Logger)({
	exitOnError: false,
	transports: [
		new (winston.transports.Console)({level: "debug", colorize: true, timestamp: true})//,
		//new (winston.transports.File)({ filename: 'info.log' })
	]//,
	// exceptionHandlers: [
	// 	new winston.transports.File({ filename: 'exceptions.log' })
	// ]
 });
// - Commander (Command Line Utility)
var cdxgc_gen_args = require("commander");

// ----------------------------
// GLOBALS:
// ----------------------------
var AMQP_EXCHANGE = "topic_cdxtasks";
var AMQP_BASE_PATH = "/opt/cdxrabbitmq";
var AMQP_OPTS = {
  cert: fs.readFileSync(AMQP_BASE_PATH + '/core_server/cert.pem'),
  key: fs.readFileSync(AMQP_BASE_PATH + '/core_server/key.pem'),
  // cert and key or
  // pfx: fs.readFileSync('../etc/client/keycert.p12'),
  passphrase: 'kJppRZYkdm4Fc5xr',
  ca: [fs.readFileSync(AMQP_BASE_PATH + '/cacert.pem')]
};

var POSSIBLE_LARGE_SEEDS = [
'80Ez$1q{t/<obTqS!.(bNpW%%jZE{,v~zBtachaB$axT4Y*;,~Q2XfK_O@x5lr1<F%;nK@CxChsB*;9sGA3g(xlZ4d|K%4n9oxGYQzcp0_z$6]A8qk,hH33(c%!AqpxxQ<Em%oZSp{xBNS2u3z(PO]x=/-KQ$R()%h_R5eaV%EMUsoVg^3Y?xqUV6{%c0ea>]k@dJ8W},E~i7H]n3cOHg$)aS%:K.Z$ar$I-ii4eHlVS&YWsZA[O639arx_FLuvz',

'o|>E{BLC/_#p))/x<Od3hK7h45V=vxO>^Cp%IvEx|YI?A;@:?u~gK~!f;k>^5XbW;U;{ji3jt5#.R,XyZ(.;!0?B,fnM|/Lkb],]<P{Ah>s3KAPW2^X*/uYg#cES_@}cQ>Mt^U_x@{+u7xCG[/%Pk,o.b*+}9AD&Lx)SeWabBg~pwB?]B/qbWZ&cYoeMx*_%bxk_?toV+,7h]g^k_x3sx+ahT@Lphx}~,x3xH336VkBAxjA_eu=*7N3+kMlonD-h',

'}IOt1_xENR0@oJsD0AG|EE(jKn&4<EJ=p[a[Y*is0Qx.jv)pg_=4@-Z$o)$=23x.S~w;WG}wX3jx#P2RuoR-=gxaMW&&}QOk!sii5=+sz?w3?LI98>*f3ljhfT3|~[Ag<jEMmxF!#iW&IYFO^@27nz}G/~T*Ex3U3t,Yxh8b/>K3U|)MQ-/6T.heLhkJjeg3{u*s7(4y5#(0mqo<Js7Qn}y|4E{TK!;{kk5,9LxxD@VtSvE&5W8OoyrrxY@iGP>',

'%O,61ZoH.hE$}K$5C?w,N(d+vqF0@)P/%;cHVSm@53B[0PKcw^u/K961=U-06ZtFEsxY*4H#@3c+]Vw}P}l9U+o}wu:K.C:=}R9E~Bcmu)X6t+:phnW}/xJG)3^1Fqdt:+~Ddog>|hT8%1LTdtj^N)szs42kx.!xGi}8[%^4?E~-QzxL3|AI,]1xOC{L<Cs+b~u4Md>[xxfZAd8r)[Srj#OI8@!X(gB^}dR=)VxRQ(XsM9adF?_z:E-.33H<7RH',

'xre{}K$O#Z~o$wel68ZU/ow?!y-erZS&&um%fI%aHyTk](UVZIxgwcQ/(b/sviXA5wQ5A8rL-I#lS4b)1V}xhw7%{8h3xs$4P_Pc)Wp<WCyhp<TxjGPY<[eJ8;:7UAxC/g$Q5kVEwV551IX*~qBXuxcQ5gKx1(LOJa~8O)aYMYOE,TgI;(D!i[0~_ohr{@~b[WzC[R;bK4A9~l5}E%@opa{]S1-Z+reMnksvK3](o_uqb3/]x%XLz~c>2&RNm{-,mx'
];
var CHOOSEN_SEED = 0;
var UNDERGRAD_SCHOOLS = ["all","navy", "marines", "army", "airforce"];
var GRAD_SCHOOLS = ["nps"];
var MIN_TIME = 200; // in milliseconds
var MAX_TIME = 300000; // 5 mins = 300000 milliseconds
var TASK_GEN_CYCLE_TIME = 500; // in milliseconds
var URL_PREFACE = "http://";
var URL_LIST = [];
var cycleTimer = null;
// ----------------------------
// Commander:
// ----------------------------

cdxgc_gen_args
	.version(version)
	.option('-a, --amqp <server name or IP>', 'AMQP Server', 'localhost')
	.option('-i, --inputFile <file>', 'Input file of URL\'s available to send')
	.option('-u, --urlPreface <format>', 'Add URL Preface', URL_PREFACE)
	.option('-c, --csvFormat <format>', 'Specify CSV Header Format')
	.option('-r, --doRandomTest', 'Print out 100 Random Counts and Times')
	.option('-m, --maxTime <time: ms>', 'Set Maximum Time in milliseconds a single job will last for. Default: 5 mins = 300000 ms.', MAX_TIME)
	.option('-n, --minTime <time: ms>', 'Set Minimum Time in milliseconds a single job will last for. Default: 200 ms. If randomly selected time is less then minimum time then minimum time is added to the randomly selected time.', MIN_TIME)
	.option('-t, --cycleTime <time: ms>', 'Set Time in milliseconds to generate a new task. Default: 500 ms.', TASK_GEN_CYCLE_TIME)
	.parse(process.argv);

// ----------------------------
// Functions:
// ----------------------------

var start = function() {
	// 		console.log(util.inspect(cdxgc_gen_args.inputFile, {color: true, showHidden: true, depth: null }));
	var deferred = when.defer();
	
	logger.info("Starting CDX GC Job Generator");
	
	// Error out if we don't get an input file.
	if (!_.isString(cdxgc_gen_args.inputFile)) {
		logger.error('Input File is REQUIRED! Please specify an input file using the \'-i <file>\' or \'--inputFile <file>\'.');
		process.exit(1); // May need to kick out.
	}
	
	if (cdxgc_gen_args.minTime <= 0) {
		logger.error('Minimum Time specified is less than or equal to 0. Specify a minimum time greater than 0!');
		process.exit(1); // May need to kick out.
	}
	
	if (cdxgc_gen_args.maxTime <= 0) {
		logger.error('Maximum Time specified is less than or equal to 0. Specify a minimum time greater than 0!');
		process.exit(1); // May need to kick out.
	}
	
	if (cdxgc_gen_args.maxTime <= cdxgc_gen_args.minTime) {
		logger.error('Maximum Time is less than or equal to Minimum time. Specify a minimum time less than maximum time or a maximum time that is greater than the minimum time!');
		process.exit(1); // May need to kick out.
	}
	
	if (cdxgc_gen_args.cycleTime < 200) {
		logger.error('Task cycle time must be at least 200 milliseconds. Please specify a number equal or greater than 200.');
		process.exit(1); // May need to kick out.
	}
	
	logger.debug("Input File Path: " + cdxgc_gen_args.inputFile);
	logger.debug("URL Preface: " + cdxgc_gen_args.urlPreface);
	logger.debug("CSV Format: " + cdxgc_gen_args.csvFormat);
	logger.debug("Max Time: " + cdxgc_gen_args.maxTime);
	logger.debug("Min Time: " + cdxgc_gen_args.minTime);
	logger.debug("Cycle Time: " + cdxgc_gen_args.cycleTime);

	// Resolver:
	deferred.resolve(cdxgc_gen_args.inputFile);
	
	return deferred.promise;
};

var csvparse = function(inputFile) { // CSV Pars
	var deferred = when.defer();
	
	var urlPreface = null;
	var csvFormat = true;
	if (_.isString(cdxgc_gen_args.urlPreface)) {
		urlPreface = cdxgc_gen_args.urlPreface;
		logger.debug("URL Preface: " + urlPreface);
	}
	if (_.isString(cdxgc_gen_args.csvFormat)) {
		csvFormat = cdxgc_gen_args.csvFormat.split(',');
		logger.debug("CSV Header Format Specified: " + csvFormat);
	}
	
	csvparser()
		.from(inputFile, {columns: csvFormat}) // Read in the input list.
		.transform(function (row, index) {
			if(row.URL.toLowerCase() == "url") {
				return null;
			}
			
			if(!_.isNull(urlPreface)) {
				row.URL = urlPreface + row.URL;
			}
			return row;
		})
		.to.array(function(data, count) {
			logger.debug("Input Count: " + count);
			deferred.resolve(data);
		});
		
	return deferred.promise;
};

var getURLandWorkTime = function(inputList) {
	
	var URL_pick = mainrand(inputList.length);
	logger.debug("URL Pick: " + URL_pick);
	var currentTime = parseInt(mainrand(cdxgc_gen_args.maxTime));
	if (currentTime < cdxgc_gen_args.minTime) {
		logger.debug("Min Time Hit - Adding MIN_TIME (" + cdxgc_gen_args.minTime + ") to value ("+ currentTime +").");
		currentTime += parseInt(cdxgc_gen_args.minTime);
	}
	logger.debug("Time Pick: " + currentTime);
	
	return new Array(URL_pick, currentTime);
};

var getTaskID = function(workArray) {
	
	var currentDate = new Date();
	var currentDigest = workArray.toString() + currentDate.toJSON() + POSSIBLE_LARGE_SEEDS[CHOOSEN_SEED];
	var shasum = crypto.createHash('sha1');
	shasum.update(currentDigest, 'utf8');
	var hashout = shasum.digest('hex');
	logger.debug("TaskID Input: " + currentDigest);
	logger.debug("TaskID SHA1: " + hashout);
	
	return hashout;
};

var getRoutingKey = function (school, secondaryPath) {
	var out = null;
	var choosenSchool = _.indexOf(UNDERGRAD_SCHOOLS, school);
	if ( choosenSchool > -1) {
		out = UNDERGRAD_SCHOOLS[choosenSchool];
		if (!_.isString(secondaryPath)) {
			out += ".task";
		} else {
			out += "." + secondaryPath;
		}
	} else {
		logger.warn("Invalid School Specified: "+school+" :: This shouldn't happen...");
	}
	return out;
};

// ----------------------------
// Runner:
// ----------------------------

start()
.then(csvparse)
.then(function (inputList) { // Start Random Setup:
	
	// Setup InputList to be generally available:
	URL_LIST = inputList;
	
	logger.silly(util.inspect(URL_LIST, {color: true, showHidden: true, depth: null }));
	logger.debug("InputList Length: " + URL_LIST.length);
	
	//Random Setup:
	CHOOSEN_SEED = well.randInt(0, (POSSIBLE_LARGE_SEEDS.length-1));
	logger.info("Choosen Seed [" + CHOOSEN_SEED + "]: " + POSSIBLE_LARGE_SEEDS[CHOOSEN_SEED]);
	
	mainrand = randgen.create(POSSIBLE_LARGE_SEEDS[CHOOSEN_SEED]);
	
	// Random Test:
	if (cdxgc_gen_args.doRandomTest) {
		for (var i=0;i<100;i++) {
			logger.debug("-->Pick "+ i +": Start");
			var URLandWorkTime = getURLandWorkTime(URL_LIST);
			logger.info("URL,WorkTime: " + URLandWorkTime);

			//Task ID:
			var currentTaskID = getTaskID(URLandWorkTime);
			logger.info("Task ID: " + currentTaskID);
			logger.debug("-->Pick "+ i +": Stop");
		}
	}


	//return amqpServer;
})
.then(function () {
	
	// Connect AMQP:
	//var amqpServer = amqp.connect('amqps://' + cdxgc_gen_args.amqp, AMQP_OPTS);
	var amqpServer = amqp.connect('amqp://localhost:5672');
	//console.log(util.inspect(amqpServer, {color: true, showHidden: true, depth: null }));
	
	amqpServer.then(function (amqpConn) {
		// Setup signals:
		process.on('SIGINT', function () {
			logger.info('SIGNAL: SIGINT caught: Closing connection.');
			clearInterval(cycleTimer);
			amqpConn.close();
		});
		process.on('SIGTERM', function () {
			logger.info('SIGNAL: SIGTERM caught: Closing connection.');
			clearInterval(cycleTimer);
			amqpConn.close();
		});
		
		return amqpConn.createChannel().then(function(ch) {
			var ok = ch.assertExchange(AMQP_EXCHANGE, 'topic', {durable: false});
			return ok.then(function() {
				var counter = 0;
				cycleTimer = setInterval(function () {
			
					var currentKey = getRoutingKey("all");
					logger.info("Routing Key: " + currentKey);
					// Pick URL and Work Time:
					logger.debug("-->Pick "+ counter +": Start");
					var URLandWorkTime = getURLandWorkTime(URL_LIST);
					logger.info("URL,WorkTime: " + URLandWorkTime);

					//Task ID:
					var currentTaskID = getTaskID(URLandWorkTime);
					logger.info("Task ID: " + currentTaskID);
			
					// Put message together:
					var msg_to_send = {};
					msg_to_send.taskid = currentTaskID;
					msg_to_send.urlNum = URL_LIST[URLandWorkTime[0]].Rank;
					msg_to_send.url = URL_LIST[URLandWorkTime[0]].URL;
					msg_to_send.workTime = URLandWorkTime[1];
			
					// Prepare message for sending:
					var msgStr = JSON.stringify(msg_to_send);
					ch.publish(AMQP_EXCHANGE, currentKey, new Buffer(msgStr));
					logger.info("Sent %s:'%s'", currentKey, msgStr);
					
					//Update Counter:
					logger.debug("-->Pick "+ counter +": Stop");
					counter++;
				},cdxgc_gen_args.cycleTime);
		
				// ch.publish(ex, key, new Buffer(message));
				// console.log(" [x] Sent %s:'%s'", key, message);
				// return ch.close();
			});
		});
		
	}).then(null,logger.error);
	
	// Set up message generation cycle time:
	//return when().ensure(function() { amqpConn.close(); })	
});
//.then(null,logger.error("Connection to AMQP Server failed."));



