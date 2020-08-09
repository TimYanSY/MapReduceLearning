from mrjob.job import MRJob
from mrjob.step import MRStep


class MostPopularSuperHero(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_friends_count,
                   reducer=self.reducer_count_firends),
            MRStep(mapper=self.mapper_passthrough,
                   reducer = self.reducer_find_max)
        ]

    def mapper_get_friends_count(self, _, line):
        friends = line.split(' ')
        hero = friends.pop(0)
        numFriends = len(friends) - 1
        yield int(hero), int(numFriends)

    def reducer_count_firends(self, key, values):
        yield None, (sum(values), "CAPTAIN AMERICANO")

    def mapper_passthrough(self, key, value):
        yield key, value

    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MostPopularSuperHero.run()
