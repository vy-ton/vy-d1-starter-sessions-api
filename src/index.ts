import { D1Database, D1DatabaseSession } from "@cloudflare/workers-types";

export type Env = {
	DB01: D1Database;
};

export default {
	async fetch(request, env, ctx): Promise<Response> {
		const url = new URL(request.url);

		// A. Create the Session.
		// When we create a D1 Session, we can continue where we left off from a previous    
		// Session if we have that Session's last bookmark or use a constraint.
		const bookmark = request.headers.get('x-d1-bookmark') ?? 'first-unconstrained';
		const session = env.DB01.withSession(bookmark);

		try {
			// Use this Session for all our Workers' routes.
			const response = await handleRequest(request, session);

			if (response.status === 200) {
				// B. Return the bookmark so we can continue the Session in another request.
				response.headers.set('x-d1-bookmark', session.getBookmark() ?? "");
			}
			return response;

		} catch (e) {
			console.error({ message: "Failed to handle request", error: String(e), errorProps: e, url, bookmark });
			return Response.json(
				{ error: String(e), errorDetails: e },
				{ status: 500 }
			);
		}
	},
} satisfies ExportedHandler<Env>;

type Order = {
	orderId: string,
	customerId: string,
	quantity: number,
}

async function handleRequest(request: Request, session: D1DatabaseSession) {
	const { pathname } = new URL(request.url);
	const tsStart = Date.now();

	// Move to migrations for efficiency.
	await initTables(session);

	if (request.method === "GET" && pathname === '/api/orders') {
		// C. Session read query.
		const resp = await session.prepare('SELECT * FROM Orders').all();
		return Response.json(buildResponse(session, resp, tsStart));

	} else if (request.method === "POST" && pathname === '/api/orders') {
		const order = await request.json<Order>();

		// D. Session write query.
		// Since this is a write query, D1 will transparently forward the query.
		await session
			.prepare('INSERT INTO Orders VALUES (?, ?, ?)')
			.bind(order.customerId, order.orderId, order.quantity)
			.run();

		// E. Session read-after-write query.
		// In order for the application to be correct, this SELECT
		// statement must see the results of the INSERT statement above.
		const resp = await session
			.prepare('SELECT COUNT(*) FROM Orders')
			.all();

		return Response.json(buildResponse(session, resp, tsStart));

	} else if (request.method === "DELETE" && pathname === '/api/orders') {
		await session
			.prepare('DROP TABLE IF EXISTS Orders;')
			.run();
		const resp = await initTables(session);
		
		return Response.json(buildResponse(session, resp, tsStart));
	}

	return new Response('Not found', { status: 404 })
}

function buildResponse(session: D1DatabaseSession, res: D1Result, tsStart: number) {
	return {
		d1Latency: Date.now() - tsStart,

		results: res.results,
		servedByRegion: res.meta.served_by_region ?? "",
		servedByPrimary: res.meta.served_by_primary ?? "",

		// Add the session bookmark inside the response body too.
		sessionBookmark: session.getBookmark(),
	};
}

async function initTables(session: D1DatabaseSession) {
	return await session
		.prepare(`CREATE TABLE IF NOT EXISTS Orders(
			customerId TEXT NOT NULL,
			orderId TEXT NOT NULL,
			quantity INTEGER NOT NULL,
			PRIMARY KEY (customerId, orderId)
		)`)
		.all();
}
