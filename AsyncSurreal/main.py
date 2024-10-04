import AsyncSurreal
import asyncio


async def main():

    pool = await AsyncSurreal.create_pool('localhost', 8000, 'user', 'password', 'namespace', 'database')

    result = await pool.query('SELECT * FROM table_name')
    print(result)

    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())