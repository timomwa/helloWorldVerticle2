package com.hello.world.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

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
	
	protected final int SERVER_PORT = 8787;
	protected JDBCPool pool;
	private final String DAD_JOKE_TABLE = "dad_joke211";
	private final String SQL_CREATE_DAD_JOKE_TABLE = "CREATE TABLE IF NOT EXISTS "+DAD_JOKE_TABLE+"(id INT NOT NULL AUTO_INCREMENT, joke VARCHAR(350) NOT NULL)";
	private final String SQL_PREP_STATEMENT_ADD_DAD_JOKE = "INSERT INTO "+DAD_JOKE_TABLE+"(joke) VALUES(?)";
	private final String SQL_GET_RANDOM_DAD_JOKE = "SELECT joke from "+DAD_JOKE_TABLE+" ORDER BY RANDOM() LIMIT 1";

	@Override
	public void start(Promise<Void> startPromise) throws Exception {
		setup();
		vertx.createHttpServer()
		.requestHandler(
				req -> {
					
					this.pool
					.query(SQL_GET_RANDOM_DAD_JOKE)
					.execute()
					.onFailure(handler->{
						System.out.println("Error performing SQL -> : "+handler.getMessage());
					})
					.onSuccess(rowSet->{
						Row row = rowSet.iterator().next();
						String randomDadJoke = row.getString(0);
						req
						.response()
						.putHeader("content-type", "text/plain")
						.end(randomDadJoke);
					});
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
		
	}

	/**
	 * Adds dad jokes to the database from 
	 * com.hello.world.verticle.DadJokes
	 */
	private void addDadJokes() {
		
		DadJokes
		.jokes
		.forEach( joke -> {
			this.pool
			.preparedQuery(SQL_PREP_STATEMENT_ADD_DAD_JOKE)
			.execute(Tuple.of(joke))
			.onSuccess(rows ->{
			}).onFailure(fl->{
			});
		});
		
	}

	/**
	 * Creates the dad jokes database
	 */
	private void createDatabase() {
		this.pool
		.query(SQL_CREATE_DAD_JOKE_TABLE)
		.execute()
		.onFailure(e->{
			System.out.println("\n\r\t🔥 Failed to create Dad Jokes Database Error: "+e.getMessage());
		}).onSuccess(rows ->{
			System.out.println("\n\r\t✅ Dad Jokes Database Created. ");
			
			//Add Dad Jokes to the database
			addDadJokes();
		});
	}

	/**
	 * Create a database connection pool
	 * 
	 */
	private void createH2DbPool() {
		final JsonObject config = new JsonObject()
				  .put("url", "jdbc:h2:~/test2")
				  .put("username", "sa")
				  .put("password", "")
				  .put("driver_class", "org.h2.Driver")
				  .put("initial_pool_size", 3)
				  .put("max_pool_size", 5);
		this.pool = JDBCPool.pool(vertx, config);
		this.pool.getConnection();
		System.out.println("\n\r\t✅ H2 Db pool initialized Successfully. ");
	}
}