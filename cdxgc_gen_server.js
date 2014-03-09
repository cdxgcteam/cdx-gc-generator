#!/usr/bin/env node
// ----------------------------
// Info:
// ----------------------------
// Title: cdxgc_gen_server.js
// Description: CDX Grey Cell Tasking Generator Server
// Author: Derek Yap <zangzi@gmail.com>
// License: MIT
// Version: 0.0.1
var version = '0.0.1';

// ----------------------------
// Requires:
// ----------------------------
// - Built-ins:
var util = require('util');
var fs = require('fs');
var crypto = require('crypto');
var os = require('os');

// - Underscore
var _ = require('underscore');

// - Commander (Command Line Utility)
var cdxgc_gen_args = require('commander');

// - Random number generators:
var randgen = require('random-seed');
var mainrand = null;
var wellprng = require('well-rng');
var well = new wellprng();

// - Redis -> Redis Driver/Adapter
var redis = require('redis');
var redisclient_msg = null;
var redisclient = null;

// - AMQP -> RabbitMQ Connection Library
var amqp = require('amqplib');
var coreChannel = null;

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
		new (winston.transports.Console)({level: 'debug', colorize: true, timestamp: true})//,
		//new (winston.transports.File)({ filename: 'info.log' })
	]//,
	// exceptionHandlers: [
	// 	new winston.transports.File({ filename: 'exceptions.log' })
	// ]
 });

// ----------------------------
// GLOBALS:
// ----------------------------
var SOURCE_SYSTEM_NAME = 'cdxgenerator';
var AMQP_EXCHANGE = 'topic_cdxtasks';
var BASE_CERT_PATH = '/home/cdxgcserver/cdx_gc_certs2';
var AMQP_OPTS = {
	cert: fs.readFileSync(BASE_CERT_PATH + '/generator/cert.pem'),
	key: fs.readFileSync(BASE_CERT_PATH + '/generator/key.pem'),
	// cert and key or
	// pfx: fs.readFileSync('../etc/client/keycert.p12'),
	//passphrase: 'kJppRZYkdm4Fc5xr',
	ca: [fs.readFileSync(BASE_CERT_PATH + '/rmqca/cacert.pem')]
};
var AMQP_PORT = 5671;
var REDIS_PORT = 6379;
var REDIS_HOST = '127.0.0.1';

var POSSIBLE_LARGE_SEEDS = [
'80Ez$1q{t/<obTqS!.(bNpW%%jZE{,v~zBtachaB$axT4Y*;,~Q2XfK_O@x5lr1<F%;nK@CxChsB*;9sGA3g(xlZ4d|K%4n9oxGYQzcp0_z$6]A8qk,hH33(c%!AqpxxQ<Em%oZSp{xBNS2u3z(PO]x=/-KQ$R()%h_R5eaV%EMUsoVg^3Y?xqUV6{%c0ea>]k@dJ8W},E~i7H]n3cOHg$)aS%:K.Z$ar$I-ii4eHlVS&YWsZA[O639arx_FLuvz',

'o|>E{BLC/_#p))/x<Od3hK7h45V=vxO>^Cp%IvEx|YI?A;@:?u~gK~!f;k>^5XbW;U;{ji3jt5#.R,XyZ(.;!0?B,fnM|/Lkb],]<P{Ah>s3KAPW2^X*/uYg#cES_@}cQ>Mt^U_x@{+u7xCG[/%Pk,o.b*+}9AD&Lx)SeWabBg~pwB?]B/qbWZ&cYoeMx*_%bxk_?toV+,7h]g^k_x3sx+ahT@Lphx}~,x3xH336VkBAxjA_eu=*7N3+kMlonD-h',

'}IOt1_xENR0@oJsD0AG|EE(jKn&4<EJ=p[a[Y*is0Qx.jv)pg_=4@-Z$o)$=23x.S~w;WG}wX3jx#P2RuoR-=gxaMW&&}QOk!sii5=+sz?w3?LI98>*f3ljhfT3|~[Ag<jEMmxF!#iW&IYFO^@27nz}G/~T*Ex3U3t,Yxh8b/>K3U|)MQ-/6T.heLhkJjeg3{u*s7(4y5#(0mqo<Js7Qn}y|4E{TK!;{kk5,9LxxD@VtSvE&5W8OoyrrxY@iGP>',

'%O,61ZoH.hE$}K$5C?w,N(d+vqF0@)P/%;cHVSm@53B[0PKcw^u/K961=U-06ZtFEsxY*4H#@3c+]Vw}P}l9U+o}wu:K.C:=}R9E~Bcmu)X6t+:phnW}/xJG)3^1Fqdt:+~Ddog>|hT8%1LTdtj^N)szs42kx.!xGi}8[%^4?E~-QzxL3|AI,]1xOC{L<Cs+b~u4Md>[xxfZAd8r)[Srj#OI8@!X(gB^}dR=)VxRQ(XsM9adF?_z:E-.33H<7RH',

'xre{}K$O#Z~o$wel68ZU/ow?!y-erZS&&um%fI%aHyTk](UVZIxgwcQ/(b/sviXA5wQ5A8rL-I#lS4b)1V}xhw7%{8h3xs$4P_Pc)Wp<WCyhp<TxjGPY<[eJ8;:7UAxC/g$Q5kVEwV551IX*~qBXuxcQ5gKx1(LOJa~8O)aYMYOE,TgI;(D!i[0~_ohr{@~b[WzC[R;bK4A9~l5}E%@opa{]S1-Z+reMnksvK3](o_uqb3/]x%XLz~c>2&RNm{-,mx'
];
var CHOOSEN_SEED = 0;
var UNDERGRAD_SCHOOLS = ['all','navy', 'marines', 'army', 'airforce'];
var COMMANDS_AVAILABLE = ['execute_url','pause', 'quit', 'execute_app'];
var GRAD_SCHOOLS = ['nps'];
var MIN_TIME = 200; // in milliseconds
var MAX_TIME = 300000; // 5 mins = 300000 milliseconds
var TASK_GEN_CYCLE_TIME = 500; // in milliseconds
var URL_PREFACE = 'http://';
var URL_LIST = [];
var CYCLE_TIMER = null;
var COUNTERS = {
	execute_url_cnt: 0,
	pause_cnt: 0,
};
var REDIS_SENT_HEADER = 'sent_meta_';
var REDIS_SENT_ORDER_KEY = 'cdx_sent_order';
var REDIS_MAL_HEADER = 'mal_meta_';
var REDIS_MAL_ORDER_KEY = 'cdx_mal_order';
var REDIS_CMD_SUBSCRIPTION = 'redis_cmd_sub';
var REDIS_MAL_SUBSCRIPTION = 'redis_mal_sub';
var MAL_TASK_QUEUE = new Array();

// ----------------------------
// Commander:
// ----------------------------

cdxgc_gen_args
	.version(version)
	.option('-ah, --amqp_host <server name or IP>', 'AMQP Server Host', os.hostname())
	.option('-ap, --amqp_port <port number>', 'AMQP Server Port', AMQP_PORT)
	.option('-rh, --redis_host <server name or IP>', 'Redis Server Host', REDIS_HOST)
	.option('-rp, --redis_port <port number>', 'Redis Server Port', REDIS_PORT)
	.option('-i, --inputFile <file>', 'Input file of URL\'s available to send')
	.option('-u, --urlPreface <format>', 'Add URL Preface', URL_PREFACE)
	.option('-c, --csvFormat <format>', 'Specify CSV Header Format')
	.option('-r, --doRandomTest', 'Print out 100 Random Counts and Times')
	.option('-m, --maxTime <time: ms>', 'Set Maximum Time in milliseconds a single job will last for. Default: 5 mins = 300000 ms.', MAX_TIME)
	.option('-n, --minTime <time: ms>', 'Set Minimum Time in milliseconds a single job will last for. Default: 200 ms. If randomly selected time is less then minimum time then minimum time is added to the randomly selected time.', MIN_TIME)
	.option('-t, --cycleTime <time: ms>', 'Set Time in milliseconds to generate a new task. Default: 500 ms.', TASK_GEN_CYCLE_TIME)
	.parse(process.argv);

// ----------------------------
// Core Functions:
// ----------------------------

var start = function() {
	// 		console.log(util.inspect(cdxgc_gen_args.inputFile, {color: true, showHidden: true, depth: null }));
	var deferred = when.defer();
	
	logger.info('Starting CDX GC Job Generator');
	
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
	
	logger.info('AMQP Server: ' + cdxgc_gen_args.amqp_host);
	logger.info('AMQP Port: ' + cdxgc_gen_args.amqp_port);
	logger.info('Redis Server: ' + cdxgc_gen_args.redis_host);
	logger.info('Redis Port: ' + cdxgc_gen_args.redis_port);
	logger.debug('Input File Path: ' + cdxgc_gen_args.inputFile);
	logger.debug('URL Preface: ' + cdxgc_gen_args.urlPreface);
	logger.debug('CSV Format: ' + cdxgc_gen_args.csvFormat);
	logger.debug('Max Time: ' + cdxgc_gen_args.maxTime);
	logger.debug('Min Time: ' + cdxgc_gen_args.minTime);
	logger.debug('Cycle Time: ' + cdxgc_gen_args.cycleTime);

	// Resolver:
	deferred.resolve(cdxgc_gen_args.inputFile);
	
	// Redis Setup:
	redisclient = redis.createClient(cdxgc_gen_args.redis_port, cdxgc_gen_args.redis_host);
	redisclient_msg = redis.createClient(cdxgc_gen_args.redis_port, cdxgc_gen_args.redis_host);
	redisclient.on('ready', function() {
		logger.info('Redis :: Main Redis Connection Ready.');
	});
	redisclient_msg.on('ready', function() {
		logger.info('Redis :: Reciever Redis Connection Ready.');
	});
    redisclient_msg.on('error', function (err) {
        logger.error('Redis Error :: ' + err);
    });
	redisclient_msg.on('message', redisCmdRecieve);
	redisclient_msg.subscribe(REDIS_CMD_SUBSCRIPTION);
	
	return deferred.promise;
};

var csvparse = function(inputFile) { // CSV Pars
	var deferred = when.defer();
	
	var urlPreface = null;
	var csvFormat = true;
	if (_.isString(cdxgc_gen_args.urlPreface)) {
		urlPreface = cdxgc_gen_args.urlPreface;
		logger.debug('csvparse :: URL Preface: ' + urlPreface);
	}
	if (_.isString(cdxgc_gen_args.csvFormat)) {
		csvFormat = cdxgc_gen_args.csvFormat.split(',');
		logger.debug('csvparse :: CSV Header Format Specified: ' + csvFormat);
	}
	
	csvparser()
		.from(inputFile, {columns: csvFormat}) // Read in the input list.
		.transform(function (row, index) {
			if(row.URL.toLowerCase() == 'url') {
				return null;
			}
			
			if(!_.isNull(urlPreface)) {
				row.URL = urlPreface + row.URL;
			}
			return row;
		})
		.to.array(function(data, count) {
			logger.debug('csvparse :: Input Count: ' + count);
			deferred.resolve(data);
		});
		
	return deferred.promise;
};

var redisCmdRecieve = function (channel, message) {
	logger.debug('redisCmdRecieve :: channel: ' + channel + ' :: msg: ' + message);
};

var mainTaskExecutor = function () {

	var taskInfo = getTasking(URL_LIST, 'execute_url');
	if (!_.isNull(taskInfo)) {
		var currentKey = getRoutingKey('all');
		logger.info('Routing Key: ' + currentKey);

		// Pick URL and Work Time:
		//logger.info('-->Pick '+ COUNTERS.execute_url_cnt +': Start');
		var taskObj = taskInfo[1];
		
		// Add the info to Redis server:
		var fullKey = REDIS_SENT_HEADER + taskObj.taskid;
		logger.info('REDIS :: Sent Key: ' + fullKey);
		redisclient.hmset(fullKey, taskObj);
		logger.info('REDIS :: Put in sort order: ' + fullKey + ' :: Time(ms): ' + taskObj.taskCreateMS);
		redisclient.hmset(fullKey, taskObj);
		
		// Set in sent key order:
		redisclient.zadd(REDIS_SENT_ORDER_KEY, taskObj.taskCreateMS, fullKey);
		
		coreChannel.publish(AMQP_EXCHANGE, currentKey, new Buffer(taskInfo[0]));
		logger.info('Sent %s:"%s"', currentKey, taskInfo[0]);
		
		//Update Counter:
		//logger.info('-->Pick '+ COUNTERS.execute_url_cnt +': Stop');
	} else {
		logger.warn('Problem creating task.');
	}
	
};


// ----------------------------
// Utils:
// ----------------------------

var getURLandWorkTime = function(inputList) {
	
	var URL_pick = mainrand(inputList.length);
	logger.debug('getURLandWorkTime :: URL Pick: ' + URL_pick);
	var currentTime = parseInt(mainrand(cdxgc_gen_args.maxTime));
	if (currentTime < cdxgc_gen_args.minTime) {
		logger.debug('getURLandWorkTime :: Min Time Hit - Adding MIN_TIME (' + cdxgc_gen_args.minTime + ') to value ('+ currentTime +').');
		currentTime += parseInt(cdxgc_gen_args.minTime);
	}
	logger.debug('Time Pick: ' + currentTime);
	
	return new Array(URL_pick, currentTime);
};

var getTaskID = function(workArray) {
	
	var currentDate = new Date();
	var currentDigest = workArray.toString() + currentDate.toJSON() + POSSIBLE_LARGE_SEEDS[CHOOSEN_SEED];
	var shasum = crypto.createHash('sha1');
	shasum.update(currentDigest, 'utf8');
	var hashout = shasum.digest('hex');
	logger.debug('getTaskID :: TaskID CurDate: ' + currentDate.toJSON());
	logger.debug('getTaskID :: TaskID Input: ' + currentDigest);
	logger.debug('getTaskID :: TaskID SHA1: ' + hashout);
	
	return new Array(currentDate.toJSON(), hashout, currentDate.getTime());
};

var getRoutingKey = function (school, secondaryPath) {
	var out = null;
	var choosenSchool = _.indexOf(UNDERGRAD_SCHOOLS, school);
	if ( choosenSchool > -1) {
		out = UNDERGRAD_SCHOOLS[choosenSchool];
		if (!_.isString(secondaryPath)) {
			out += '.task';
		} else {
			out += '.' + secondaryPath;
		}
	} else {
		logger.warn('getRoutingKey :: Invalid School Specified: '+school+' :: This shouldn\'t happen...');
	}
	return out;
};

var getTasking = function (urlListArray, commandStr) {
	
	var msgStr = null;
	
	var choosenCommand = _.indexOf(COMMANDS_AVAILABLE,commandStr);
	if (choosenCommand == -1) {
		logger.warn('Invalid Command Specified: '+commandStr+' :: This shouldn\'t happen...');
		return msgStr;
	}
	
	var URLandWorkTime = getURLandWorkTime(urlListArray);
	logger.info('getTasking :: URL,WorkTime: ' + URLandWorkTime);

	//Task ID:
	var currentTaskIDInfo = getTaskID(URLandWorkTime);
	logger.info('getTasking :: Task Creation Date: ' + currentTaskIDInfo[0]);
	logger.info('getTasking :: Task ID: ' + currentTaskIDInfo[1]);

	// Put message together:
	var msg_to_send = {};
	msg_to_send.srcSystem = SOURCE_SYSTEM_NAME;					//Source System Generator
	msg_to_send.poc = SOURCE_SYSTEM_NAME;						//POC == Source System Generator
	msg_to_send.cmd = COMMANDS_AVAILABLE[choosenCommand];		//Choosen Command
	msg_to_send.taskCreateDate = currentTaskIDInfo[0];			//Task Creation Date/Time
	msg_to_send.taskCreateMS = currentTaskIDInfo[2];			//Task Creation Date/Time in Milliseconds.
	msg_to_send.taskid = currentTaskIDInfo[1];					//Task ID
	msg_to_send.urlNum = urlListArray[URLandWorkTime[0]].Rank;	//URL Number/Rank
	msg_to_send.url = urlListArray[URLandWorkTime[0]].URL;		//URL Itself
	msg_to_send.minWorkTime = cdxgc_gen_args.minTime;			//Min Work Time in MS
	msg_to_send.workTime = URLandWorkTime[1];					//How long to sit and wait on page == work time.

	// Prepare message for sending:
	msgStr = JSON.stringify(msg_to_send);
	
	return new Array(msgStr, msg_to_send);
};

var printCounters = function (counters) {
	logger.info('Counters:\n' + util.inspect(counters, {color: true, showHidden: true, depth: null }))
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
	logger.debug('InputList Length: ' + URL_LIST.length);
	
	//Random Setup:
	CHOOSEN_SEED = well.randInt(0, (POSSIBLE_LARGE_SEEDS.length-1));
	logger.info('Choosen Seed [' + CHOOSEN_SEED + ']: ' + POSSIBLE_LARGE_SEEDS[CHOOSEN_SEED]);
	
	mainrand = randgen.create(POSSIBLE_LARGE_SEEDS[CHOOSEN_SEED]);
	
	// Random Test:
	if (cdxgc_gen_args.doRandomTest) {
		for (var i=0;i<100;i++) {
			logger.info('-->Pick '+ i +': Start');
			var URLandWorkTime = getURLandWorkTime(URL_LIST);
			logger.debug('URL,WorkTime: ' + URLandWorkTime);

			//Task ID:
			var currentTaskID = getTaskID(URLandWorkTime);
			logger.info('Task ID: ' + currentTaskID);
			logger.debug('-->Pick '+ i +': Stop');
		}
	}


	//return amqpServer;
})
.then(function () {
	
	// Connect AMQP:
	var amqpServer = amqp.connect('amqps://' + cdxgc_gen_args.amqp_host + ':' + cdxgc_gen_args.amqp_port, AMQP_OPTS);
	//var amqpServer = amqp.connect('amqp://localhost:5672');
	//var amqpServer = amqp.connect('amqps://cdxgcserver:5671', AMQP_OPTS);
	//console.log(util.inspect(amqpServer, {color: true, showHidden: true, depth: null }));
	
	
	amqpServer.then(function (amqpConn) {
		// Setup signals:
		process.on('SIGINT', function () {
			logger.info('SIGNAL: SIGINT caught: Closing connection.');
			clearInterval(CYCLE_TIMER);
			amqpConn.close();
			printCounters(COUNTERS);
			process.exit(1); // May need to kick out.
		});
		process.on('SIGTERM', function () {
			logger.info('SIGNAL: SIGTERM caught: Closing connection.');
			clearInterval(CYCLE_TIMER);
			amqpConn.close();
			printCounters(COUNTERS);
			process.exit(1); // May need to kick out.
		});
		
		return amqpConn.createChannel().then(function(ch) {
			coreChannel = ch;
			var ok = ch.assertExchange(AMQP_EXCHANGE, 'topic', {durable: false});
			return ok.then(function() {
				// CYCLE_TIMER = setInterval(function () {
// 			
// 					var taskStr = getTasking(URL_LIST, "execute_url");
// 					if (!_.isNull(taskStr)) {
// 						var currentKey = getRoutingKey("all");
// 						logger.info("Routing Key: " + currentKey);
// 
// 						// Pick URL and Work Time:
// 						logger.info("-->Pick "+ COUNTERS.execute_url_cnt +": Start");
// 						ch.publish(AMQP_EXCHANGE, currentKey, new Buffer(taskStr));
// 						logger.info("Sent %s:'%s'", currentKey, taskStr);
// 						
// 						//Update Counter:
// 						logger.info("-->Pick "+ COUNTERS.execute_url_cnt +": Stop");
// 					} else {
// 						logger.warn("Problem creating task.");
// 					}
// 					
// 				},cdxgc_gen_args.cycleTime);
				CYCLE_TIMER = setInterval(mainTaskExecutor,cdxgc_gen_args.cycleTime);
				// ch.publish(ex, key, new Buffer(message));
				// console.log(" [x] Sent %s:'%s'", key, message);
				// return ch.close();
			});
		});
		
	}).then(null,function (err) {
		logger.error('AMQP Error :: '+ err);
	});
});

