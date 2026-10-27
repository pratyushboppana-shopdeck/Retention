"""One-off, read-only check of the new moves query (delete after use)."""
import os, sys, collections
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import refresh as R
s = R.login()
m = R.parse_moves(R.run_csv(s, R.MOVES_SQL))
print("rows", len(m["rows"]), "sellers", len(m["sellers"]), "days", m["days"][:1], m["days"][-1:])
c = collections.Counter((m["days"][r[0]][:7], m["moves"][r[1]]) for r in m["rows"])
for mo in sorted({k[0] for k in c}):
    print(mo, {mv: c[(mo, mv)] for mv in m["moves"]})
# rerun
