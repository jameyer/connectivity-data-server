### Setup
1. Install PostgreSQL (an admin tool like pgAdmin is also useful).

2. Add PostGIS extension to the PostgreSQL installation.

3. Create a new database.

4. Add database parameters to hikariConfig in Database.kt.

5. Run Server.kt to create tables.

6. Create a GiST index manually for the "location" row in the Areas table.

```sql
CREATE INDEX areas_location ON public.areas USING gist(location);
```

7. Make sure ports set in Server.kt and UDPServer.kt are open.
