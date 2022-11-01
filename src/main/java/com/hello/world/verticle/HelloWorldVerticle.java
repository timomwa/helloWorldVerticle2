package com.hello.world.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Row;

/**
 * All Verticles extend the abstract 
 * class io.vertx.core.AbstractVerticle
 * 
 * This Hello World example creates a simple HTTP server,
 * that will accept connections from the a pre-defined port
 * 
 * @author mwangigikonyo
 *
 */
public class HelloWorldVerticle extends AbstractVerticle {
	
	private final int SERVER_PORT = 8787;
	private JDBCPool pool;

	@Override
	public void start(Promise<Void> startPromise) throws Exception {
		
		setup();
		
		
		vertx.createHttpServer().requestHandler(
				req -> {
						req
						.response()
						.putHeader("content-type", "text/plain")
						.end("Hello World!\r\nThis is from a Verticle!");
						
		}).listen(SERVER_PORT, http -> {
			
			if (http.succeeded()) {
				startPromise.complete();
				System.out.println(String.format("HTTP server started on port %s", SERVER_PORT));
			} else {
				startPromise.fail(http.cause());
			}
			
		});
	}
	

	/**
	 * Sets up the boilerplate code, not
	 * necessarily related to this Vert.x 
	 * tutorial.
	 * In this case we just want to 
	 *    -  Create the H2 in-memory db
	 * 	  -  Create the database
	 * 	  -  Add dad jokes to the database
	 */
	private void setup() {
		//Create the H2 db
		createH2DbPool();
		//Create the Database
		createDatabase();
		//Add Dad Jokes to the database
		addDadJokes();
	}

	private void addDadJokes() {
		this.pool
		.query("INSERT INTO dad_jokes4(1, 'Why didn\'t the bike move?\n\rBecause it was two tyred.')")
		.execute().onFailure(e->{
			System.out.println("\n\r\t🔥 Failed to add Dad Jokes to Database ");
		})
		.onSuccess( rows ->{
			System.out.println("\n\r\t✅ Dad Joke Added. ");
		});
		
	}

	private void createDatabase() {
		
		JDBCClient jdbcClient = JDBCClient.createShared(vertx, new JsonObject()
                .put("url", "jdbc:h2:~/test")
                .put("user", "sa")
                .put("password", "")
                .put("driver_class", "org.h2.Driver")
                .put("max_pool_size", 16));
		
		jdbcClient
		.query("CREATE TABLE dad_jokes4(id INT NOT NULL, joke VARCHAR(50) NOT NULL);", e->{
			System.out.println("\n\t\t >>> "+e);
		});
		/*
		this.pool
		.query("CREATE TABLE dad_jokes3(id INT NOT NULL, joke VARCHAR(50) NOT NULL);")
		.execute()
		.onFailure(e->{
			
			e.printStackTrace();
			System.out.println("\n\r\t🔥 Failed to create Dad Jokes Database ");
			
		}).onSuccess(rows ->{
			
			System.out.println("\n\r\t✅ Dad Jokes Database Created. ");
			System.out.println("\n\r\t✅ Rows:  "+rows.size());
			for (Row row : rows) {
			      System.out.println(row.getColumnName(1));
			 }
		});*/
		
	}

	private void createH2DbPool() {
		final JsonObject config = new JsonObject()
				  .put("url", "jdbc:h2:~/test")
				  .put("datasourceName", "pool-name")
				  .put("username", "sa")
				  .put("password", "")
				  .put("initial_pool_size", 3)
				  .put("max_pool_size", 16);
		this.pool = JDBCPool.pool(vertx, config);
		System.out.println("\n\r\t✅ H2 Db pool initialized Successfully. Initial Pool size: "+this.pool.size());
		
	}
}